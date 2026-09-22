package store

import (
	"os"
)

func mkdirAll(dir string) error { return os.MkdirAll(dir, 0o755) }

func readDirNames(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	return f.Readdirnames(-1)
}
