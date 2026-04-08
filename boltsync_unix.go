//go:build !windows && !plan9 && !linux && !openbsd

package main

// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) error {
	return db.file.Sync()
}
