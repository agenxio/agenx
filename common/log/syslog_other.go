// +build windows nacl plan9

package log

import "log"

func openSyslog(level Priority, prefix string) *log.Logger {
	return nil
}
