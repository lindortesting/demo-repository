//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly

package kv

import "os"

// ownershipEnforced reports whether Open can enforce exclusive WAL access on
// this platform.
const ownershipEnforced = false

// lockWAL is a no-op: this platform has no flock(2), so exclusive WAL
// ownership is not enforced and opening the same path twice can diverge —
// see README.
func lockWAL(*os.File) error { return nil }
