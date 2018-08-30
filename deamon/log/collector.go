package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/queueio/sentry/utils/job"
	"github.com/queueio/sentry/utils/log"
cfg	"github.com/queueio/sentry/utils/config"

	"github.com/queueio/sentry/components/scribe"
	"github.com/queueio/sentry/utils/queue"
	"github.com/queueio/sentry/utils/outputs"
)

const (
	recursiveGlobDepth = 8
)

func init() {
	err := scribe.Register("log", New)
	if err != nil {
		panic(err)
	}
}

type Collector struct {
	cfg       *cfg.Config
	config     config
	states    *scribe.States
	executor  *job.Executor
	output     queue.Handler
	done       chan struct{}
}

func New(cfg *cfg.Config, handler queue.Handler, context scribe.Context) (scribe.Collector, error) {
	c := &Collector{
		config:      defaultConfig,
		cfg:         cfg,
		executor:    job.New(),
		output:    handler,
		states:      &scribe.States{},
		done:        context.Done,
	}

	if err := cfg.Unpack(&c.config); err != nil {
		return nil, err
	}
	if err := c.config.resolvePaths(); err != nil {
		log.Err("Failed to resolve paths in config: %+v", err)
		return nil, err
	}

	_, err := c.createScanner(scribe.State{})
	if err != nil {
		return nil, err
	}

	if len(c.config.Paths) == 0 {
		return nil, fmt.Errorf("each collector must have at least one path defined")
	}

	err = c.loadStates(context.States)
	if err != nil {
		return nil, err
	}

	log.Debug("collector", "File Configs: %v", c.config.Paths)
	return c, nil
}

func (c *Collector) loadStates(states []scribe.State) error {
	log.Debug("collector", "exclude_files: %s", c.config.Exclude.Files)

	for _, state := range states {
		if c.matchesFile(state.Source) {
			state.TTL = -1

			if !state.Finished {
				return fmt.Errorf("Can only start a collector when all related states are finished: %+v", state)
			}

			err := c.updateState(state)
			if err != nil {
				log.Err("Problem putting initial state: %+v", err)
				return err
			}
		}
	}

	log.Debug("collector", "Collector with previous states loaded: %v", c.states.Count())
	return nil
}

func (c *Collector) Run() {
	log.Debug("collector", "Start next scan")

	if c.config.Tail.Files {
		ignoreOlder := c.config.Ignore.Older
		c.config.Ignore.Older = 1
		defer func() {
			c.config.Ignore.Older = ignoreOlder
			c.config.Tail.Files = false
		}()
	}

	c.visitor()

	if c.config.State.Clean.Inactive > 0 || c.config.State.Clean.Removed {
		beforeCount := c.states.Count()
		cleanedStates := c.states.Cleanup()
		log.Debug("collector", "Collector states cleaned up. Before: %d, After: %d", beforeCount, beforeCount-cleanedStates)
	}

	if c.config.State.Clean.Removed {
		for _, this := range c.states.GetStates() {
			stat, err := os.Stat(this.Source)
			if err != nil {
				if os.IsNotExist(err) {
					c.removeState(this)
					log.Debug("collector", "Remove state for file as file removed: %s", this.Source)
				} else {
					log.Err("Collector state for %s was not removed: %s", this.Source, err)
				}
			} else {
				newState := scribe.NewState(stat, this.Source, c.config.Type)
				if !newState.FileStateOS.IsSame(this.FileStateOS) {
					c.removeState(this)
					log.Debug("collector", "Remove state for file as file removed or renamed: %s", this.Source)
				}
			}
		}
	}
}

func (c *Collector) removeState(state scribe.State) {
	if !state.Finished {
		log.Debug("collector", "State for file not removed because worker not finished: %s", state.Source)
		return
	}

	state.TTL = 0
	err := c.updateState(state)
	if err != nil {
		log.Err("File cleanup state update error: %s", err)
	}
}

func (p *Collector) getFiles() map[string]os.FileInfo {
	paths := map[string]os.FileInfo{}

	for _, path := range p.config.Paths {
		matches, err := filepath.Glob(path)
		if err != nil {
			log.Err("glob(%s) failed: %v", path, err)
			continue
		}

	OUTER:
		for _, file := range matches {
			if p.isFileExcluded(file) {
				log.Debug("collector", "Exclude file: %s", file)
				continue
			}

			fileInfo, err := os.Lstat(file)
			if err != nil {
				log.Debug("collector", "lstat(%s) failed: %s", file, err)
				continue
			}

			if fileInfo.IsDir() {
				log.Debug("collector", "Skipping directory: %s", file)
				continue
			}

			isSymlink := fileInfo.Mode()&os.ModeSymlink > 0
			if isSymlink && !p.config.Symlinks {
				log.Debug("collector", "File %s skipped as it is a symlink.", file)
				continue
			}

			fileInfo, err = os.Stat(file)
			if err != nil {
				log.Debug("collector", "stat(%s) failed: %s", file, err)
				continue
			}

			if p.config.Symlinks {
				for _, finfo := range paths {
					if os.SameFile(finfo, fileInfo) {
						log.Info("Same file found as symlink and originap. Skipping file: %s", file)
						continue OUTER
					}
				}
			}

			paths[file] = fileInfo
		}
	}

	return paths
}

func (p *Collector) matchesFile(filePath string) bool {
	filePath = filepath.Clean(filePath)

	for _, glob := range p.config.Paths {
		glob = filepath.Clean(glob)

		match, err := filepath.Match(glob, filePath)
		if err != nil {
			log.Debug("collector", "Error matching glob: %s", err)
			continue
		}

		if match && !p.isFileExcluded(filePath) {
			return true
		}
	}
	return false
}

type FileSortInfo struct {
	info os.FileInfo
	path string
}

func getSortInfos(paths map[string]os.FileInfo) []FileSortInfo {
	sortInfos := make([]FileSortInfo, 0, len(paths))
	for path, info := range paths {
		sortInfo := FileSortInfo{info: info, path: path}
		sortInfos = append(sortInfos, sortInfo)
	}

	return sortInfos
}

func getSortedFiles(scanOrder string, scanSort string, sortInfos []FileSortInfo) ([]FileSortInfo, error) {
	var sortFunc func(i, j int) bool
	switch scanSort {
	case "modtime":
		switch scanOrder {
		case "asc":
			sortFunc = func(i, j int) bool {
				return sortInfos[i].info.ModTime().Before(sortInfos[j].info.ModTime())
			}
		case "desc":
			sortFunc = func(i, j int) bool {
				return sortInfos[i].info.ModTime().After(sortInfos[j].info.ModTime())
			}
		default:
			return nil, fmt.Errorf("Unexpected value for scan.order: %v", scanOrder)
		}
	case "filename":
		switch scanOrder {
		case "asc":
			sortFunc = func(i, j int) bool {
				return strings.Compare(sortInfos[i].info.Name(), sortInfos[j].info.Name()) < 0
			}
		case "desc":
			sortFunc = func(i, j int) bool {
				return strings.Compare(sortInfos[i].info.Name(), sortInfos[j].info.Name()) > 0
			}
		default:
			return nil, fmt.Errorf("Unexpected value for scan.order: %v", scanOrder)
		}
	default:
		return nil, fmt.Errorf("Unexpected value for scan.sort: %v", scanSort)
	}

	if sortFunc != nil {
		sort.Slice(sortInfos, sortFunc)
	}

	return sortInfos, nil
}

func getFileState(path string, info os.FileInfo, c *Collector) (scribe.State, error) {
	var err error
	var absolutePath string
	absolutePath, err = filepath.Abs(path)
	if err != nil {
		return scribe.State{}, fmt.Errorf("could not fetch abs path for file %s: %s", absolutePath, err)
	}
	log.Debug("collector", "Check file for harvesting: %s", absolutePath)
	newState := scribe.NewState(info, absolutePath, c.config.Type)
	return newState, nil
}

func getKeys(paths map[string]os.FileInfo) []string {
	files := make([]string, 0)
	for file := range paths {
		files = append(files, file)
	}
	return files
}

func (c *Collector) visitor() {
	var sortInfos []FileSortInfo
	var files []string

	paths := c.getFiles()

	var err error

	if c.config.Scanner.Sort != "" {
		sortInfos, err = getSortedFiles(c.config.Scanner.Order, c.config.Scanner.Sort, getSortInfos(paths))
		if err != nil {
			log.Err("Failed to sort files during scan due to error %s", err)
		}
	}

	if sortInfos == nil {
		files = getKeys(paths)
	}

	for i := 0; i < len(paths); i++ {

		var path string
		var info os.FileInfo

		if sortInfos == nil {
			path = files[i]
			info = paths[path]
		} else {
			path = sortInfos[i].path
			info = sortInfos[i].info
		}

		select {
		case <-c.done:
			log.Info("Find aborted because collector stopped.")
			return
		default:
		}

		newState, err := getFileState(path, info, c)
		if err != nil {
			log.Err("Skipping file %s due to error %s", path, err)
		}

		lastState := c.states.FindPrevious(newState)

		if c.isIgnoreOlder(newState) {
			err := c.handleIgnoreOlder(lastState, newState)
			if err != nil {
				log.Err("Updating ignore_older state error: %s", err)
			}
			continue
		}

		if lastState.IsEmpty() {
			log.Debug("collector", "Start worker for new file: %s", newState.Source)
			err := c.startScanner(newState, 0)
			if err != nil {
				log.Err("Scanner could not be started on new file: %s, Err: %s", newState.Source, err)
			}
		} else {
			c.rescan(newState, lastState)
		}
	}
}

func (c *Collector) rescan(newState scribe.State, oldState scribe.State) {
	log.Debug("collector", "Update existing file for harvesting: %s, offset: %v", newState.Source, oldState.Offset)

	if oldState.Finished && newState.Fileinfo.Size() > oldState.Offset {
		log.Debug("collector", "Resuming harvesting of file: %s, offset: %d, new size: %d", newState.Source, oldState.Offset, newState.Fileinfo.Size())
		err := c.startScanner(newState, oldState.Offset)
		if err != nil {
			log.Err("Scanner could not be started on existing file: %s, Err: %s", newState.Source, err)
		}
		return
	}

	if oldState.Finished && newState.Fileinfo.Size() < oldState.Offset {
		log.Debug("collector", "Old file was truncated. Starting from the beginning: %s, offset: %d, new size: %d ", newState.Source, newState.Fileinfo.Size())
		err := c.startScanner(newState, 0)
		if err != nil {
			log.Err("Scanner could not be started on truncated file: %s, Err: %s", newState.Source, err)
		}

		return
	}

	if oldState.Source != "" && oldState.Source != newState.Source {
		log.Debug("collector", "File rename was detected: %s -> %s, Current offset: %v", oldState.Source, newState.Source, oldState.Offset)

		if oldState.Finished {
			log.Debug("collector", "Updating state for renamed file: %s -> %s, Current offset: %v", oldState.Source, newState.Source, oldState.Offset)
			oldState.Source = newState.Source
			err := c.updateState(oldState)
			if err != nil {
				log.Err("File rotation state update error: %s", err)
			}

			//filesRenamed.Add(1)
		} else {
			log.Debug("collector", "File rename detected but worker not finished yet.")
		}
	}

	if !oldState.Finished {
		log.Debug("collector", "Scanner for file is still running: %s", newState.Source)
	} else {
		log.Debug("collector", "File didn't change: %s", newState.Source)
	}
}

func (p *Collector) handleIgnoreOlder(lastState, newState scribe.State) error {
	log.Debug("collector", "Ignore file because ignore_older reached: %s", newState.Source)

	if !lastState.IsEmpty() {
		if !lastState.Finished {
			log.Info("File is falling under ignore_older before harvesting is finished. Adjust your close_* settings: %s", newState.Source)
		}
		return nil
	}

	if p.isCleanInactive(newState) {
		log.Debug("collector", "Do not write state for ignore_older because clean_inactive reached")
		return nil
	}

	newState.Offset = newState.Fileinfo.Size()
	newState.Finished = true
	err := p.updateState(newState)
	if err != nil {
		return err
	}

	return nil
}

func (c *Collector) isFileExcluded(file string) bool {
	patterns := c.config.Exclude.Files
	return len(patterns) > 0 && scribe.MatchAny(patterns, file)
}

func (c *Collector) isIgnoreOlder(state scribe.State) bool {
	if c.config.Ignore.Older == 0 {
		return false
	}

	modTime := state.Fileinfo.ModTime()
	if time.Since(modTime) > c.config.Ignore.Older {
		return true
	}

	return false
}

func (c *Collector) isCleanInactive(state scribe.State) bool {
	if c.config.State.Clean.Inactive <= 0 {
		return false
	}

	modTime := state.Fileinfo.ModTime()
	if time.Since(modTime) > c.config.State.Clean.Inactive {
		return true
	}

	return false
}

func (c *Collector) createScanner(state scribe.State) (*Scanner, error) {
	output := outputs.GroupPublish(c.config.Name, c.output)
	return NewScanner(
		c.cfg,
		state,
		c.states,
		output,
	)
}

func (c *Collector) startScanner(state scribe.State, offset int64) error {
	if c.config.Scanner.Limit > 0 && c.executor.Len() >= c.config.Scanner.Limit {
		return fmt.Errorf("Scanner limit reached")
	}

	state.Finished = false
	state.Offset = offset

	scanner, err := c.createScanner(state)
	if err != nil {
		return err
	}

	err = scanner.Setup()
	if err != nil {
		return fmt.Errorf("Error setting up worker: %s", err)
	}

	scanner.SendStateUpdate()
	c.executor.Start(scanner)
	return nil
}

func (c *Collector) updateState(state scribe.State) error {
	if c.config.State.Inactive > 0 && state.TTL != 0 {
		state.TTL = c.config.State.Inactive
	}

	c.states.Update(state)

	data := scribe.NewData()
	data.SetState(state)
	return nil
}

func (c *Collector) Wait() {
	c.executor.WaitForCompletion()
	c.Stop()
}

func (c *Collector) Stop() {
	c.executor.Stop()
}