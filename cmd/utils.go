package cmd

import (
	"bytes"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

type ObjectPath struct {
	bucketURL *url.URL
	name      string
}

func NewObjectPath(bucketName, path string, urlTpl *template.Template) (op *ObjectPath, err error) {
	w := bytes.NewBuffer(nil)
	urlTpl.Execute(w, struct {
		BucketName string
	}{
		bucketName,
	})

	bucketURL, err := url.Parse(w.String())
	if err != nil {
		err = errors.New("bucketURL is invalid")
		return
	}

	op = &ObjectPath{
		bucketURL: bucketURL,
		name:      cleanCosPath(path),
	}

	return
}

func getFileName(path string) string {
	_, name := filepath.Split(path)
	return name
}

func cleanCosPath(path string) string {
	return strings.TrimLeft(path, "/")
}

func isDir(path string) bool {
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return true
	}
	return false
}
