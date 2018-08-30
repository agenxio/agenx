package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const RotateMaxFiles = 1024
const DefaultKeepFiles = 7
const DefaultLimit = 10 * 1024 * 1024

type Rotate struct {
	Path          string
	Name          string
	Limit        *uint64
	KeepFiles    *int
	Permissions  *uint32

	current     *os.File
	currentSize uint64
	currentLock sync.RWMutex
}

func (rotate *Rotate) CreateDirectory() error {
	fileinfo, err := os.Stat(rotate.Path)
	if err == nil {
		if !fileinfo.IsDir() {
			return fmt.Errorf("%s exists but it's not a directory", rotate.Path)
		}
	}

	if os.IsNotExist(err) {
		err = os.MkdirAll(rotate.Path, 0750)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rotate *Rotate) CheckIfConfigSane() error {
	if len(rotate.Name) == 0 {
		return fmt.Errorf("file logging requires a name for the file names")
	}
	if rotate.KeepFiles == nil {
		rotate.KeepFiles = new(int)
		*rotate.KeepFiles = DefaultKeepFiles
	}
	if rotate.Limit == nil {
		rotate.Limit = new(uint64)
		*rotate.Limit = DefaultLimit
	}

	if *rotate.KeepFiles < 2 || *rotate.KeepFiles >= RotateMaxFiles {
		return fmt.Errorf("the number of files to keep should be between 2 and %d", RotateMaxFiles-1)
	}

	if rotate.Permissions != nil && (*rotate.Permissions > uint32(os.ModePerm)) {
		return fmt.Errorf("the permissions mask %d is invalid", *rotate.Permissions)
	}
	return nil
}

func (rotate *Rotate) WriteLine(line []byte) error {
	if rotate.shouldRotate() {
		err := rotate.Rotate()
		if err != nil {
			return err
		}
	}

	line = append(line, '\n')

	rotate.currentLock.RLock()
	_, err := rotate.current.Write(line)
	rotate.currentLock.RUnlock()

	if err != nil {
		return err
	}

	rotate.currentLock.Lock()
	rotate.currentSize += uint64(len(line))
	rotate.currentLock.Unlock()

	return nil
}

func (rotate *Rotate) shouldRotate() bool {
	rotate.currentLock.RLock()
	defer rotate.currentLock.RUnlock()

	if rotate.current == nil {
		return true
	}

	if rotate.currentSize >= *rotate.Limit {
		return true
	}

	return false
}

func (rotate *Rotate) FilePath(fileNo int) string {
	if fileNo == 0 {
		return filepath.Join(rotate.Path, rotate.Name)
	}
	filename := strings.Join([]string{rotate.Name, strconv.Itoa(fileNo)}, ".")
	return filepath.Join(rotate.Path, filename)
}

func (rotate *Rotate) FileExists(fileNo int) bool {
	path := rotate.FilePath(fileNo)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (rotate *Rotate) Rotate() error {
	rotate.currentLock.Lock()
	defer rotate.currentLock.Unlock()

	if rotate.current != nil {
		if err := rotate.current.Close(); err != nil {
			return err
		}
	}

	// delete any extra files, normally we shouldn't have any
	for fileNo := *rotate.KeepFiles; fileNo < RotateMaxFiles; fileNo++ {
		if rotate.FileExists(fileNo) {
			perr := os.Remove(rotate.FilePath(fileNo))
			if perr != nil {
				return perr
			}
		}
	}

	for fileNo := *rotate.KeepFiles - 1; fileNo >= 0; fileNo-- {
		if !rotate.FileExists(fileNo) {
			continue
		}
		path := rotate.FilePath(fileNo)

		if rotate.FileExists(fileNo + 1) {
			return fmt.Errorf("file %s exists, when rotating would overwrite it", rotate.FilePath(fileNo+1))
		}

		err := os.Rename(path, rotate.FilePath(fileNo+1))
		if err != nil {
			return err
		}
	}

	path := rotate.FilePath(0)
	current, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(rotate.getPermissions()))
	if err != nil {
		return err
	}
	rotate.current = current
	rotate.currentSize = 0

	path = rotate.FilePath(*rotate.KeepFiles)
	os.Remove(path)

	return nil
}

func (rotate *Rotate) getPermissions() uint32 {
	if rotate.Permissions == nil {
		return 0600
	}
	return *rotate.Permissions
}
