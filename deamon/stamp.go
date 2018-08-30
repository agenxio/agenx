package deamon

import (
	"fmt"
	"os"
	"syscall"
)

type StateOS struct {
	Inode  uint64 `json:"inode,"`
	Device uint64 `json:"device,"`
}

func GetOSState(info os.FileInfo) StateOS {
	stat := info.Sys().(*syscall.Stat_t)

	// Convert inode and dev to uint64 to be cross platform compatible
	fileState := StateOS{
		Inode:  uint64(stat.Ino),
		Device: uint64(stat.Dev),
	}

	return fileState
}

func (fs StateOS) IsSame(state StateOS) bool {
	return fs.Inode == state.Inode && fs.Device == state.Device
}

func (fs StateOS) String() string {
	return fmt.Sprintf("%d-%d", fs.Inode, fs.Device)
}

func ReadOpen(path string) (*os.File, error) {
	flag := os.O_RDONLY
	perm := os.FileMode(0)
	return os.OpenFile(path, flag, perm)
}
