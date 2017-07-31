package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/template"
)

type task struct {
	doit func()
}

type goroutinePool struct {
	maxWorkers int
	queueSize  int
	wg         sync.WaitGroup
	queue      chan task
}

func newGoroutinePool(maxWorkers, queueSize int) *goroutinePool {
	p := &goroutinePool{
		maxWorkers: maxWorkers,
		queueSize: queueSize,
		queue:      make(chan task, queueSize),
	}
	p.startWorkers()
	return p
}

func (p *goroutinePool) clone() *goroutinePool {
	return newGoroutinePool(p.maxWorkers, p.queueSize)
}

func (p *goroutinePool) startWorkers() {
	// TODO: 可以中途停止 pool?
	for i := 0; i < p.maxWorkers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for t := range p.queue {
				t.doit()
			}
		}()
	}
}

func (p *goroutinePool) submit(f func()) {
	p.queue <- task{
		doit: f,
	}
}

func (p *goroutinePool) join() {
	close(p.queue)
	p.wg.Wait()
}

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

func exitWithError(err error) {
	fmt.Println(err)
	os.Exit(1)
}
