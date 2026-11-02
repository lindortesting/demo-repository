//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package kv

import (
	"os"
	"syscall"
)

// ownershipEnforced reports whether Open can enforce exclusive WAL access on
// this platform.
const ownershipEnforced = true

// lockWAL takes an exclusive, non-blocking advisory lock on the WAL file.
// The lock belongs to the open file description, so it conflicts with any
// store — in this or another process — opening the same file, and it is
// released automatically when the file is closed or the process dies.
func lockWAL(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}
