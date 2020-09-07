package pkg

import (
	"io/ioutil"
	"mrcroxx.io/hermes/log"
	"os"
	"path/filepath"
)

func Exist(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func Write(p string, data []byte) error {
	return ioutil.WriteFile(p, data, 0644)
}

func CleanTmp(p string) {
	if !Exist(p) {
		return
	}
	if err := filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".tmp" {
			log.ZAPSugaredLogger().Debugf("Remove .tmp file : %s", path)
			return os.Remove(path)
		}
		return nil
	}); err != nil {
		log.ZAPSugaredLogger().Fatalf("Error raised when cleaning .tmp file, err=%s.", err)
	}
}
