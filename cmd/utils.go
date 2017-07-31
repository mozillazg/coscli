package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/template"

	"github.com/mozillazg/go-cos"
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
		queueSize:  queueSize,
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

// 生成客户端 range header value 的值
// blockSize: 每块大小
// n: 第几块, n 从 0 开始
func calRange(blockSize, n int) string {
	start := blockSize * n
	end := blockSize*(n+1) - 1
	return fmt.Sprintf("bytes=%d-%d", start, end)
}

// 计算可以分多少块
func calBlock(total, blockSize int) int {
	// 分块大小
	bsize := float64(blockSize)
	// 分多少块
	nblock := 1
	if total > int(bsize) {
		nblock = int(math.Ceil(float64(total) / bsize))
	}
	return nblock
}

func getObjectsByPrefix(ctx context.Context, client *cos.Client, prefix string, maxKeys int,
	cObjs chan<- cos.Object, cErrs chan<- error) {
	next := ""
	for {
		opt := &cos.BucketGetOptions{
			Prefix:  prefix,
			MaxKeys: maxKeys,
		}
		if next != "" {
			opt.Marker = next
		}
		ret, _, err := client.Bucket.Get(ctx, opt)
		if err != nil {
			close(cObjs)
			cErrs <- err
			close(cErrs)
			return
		}

		// 文件列表
		for _, o := range ret.Contents {
			cObjs <- o
		}

		next = ret.NextMarker
		if next == "" {
			break
		}
	}
	close(cObjs)
	close(cErrs)
	return
}
