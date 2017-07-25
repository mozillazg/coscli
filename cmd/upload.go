package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mozillazg/go-cos"
	"github.com/spf13/cobra"
)

type uploader struct {
	client *cos.Client
	config struct {
		bucketURL *url.URL
		remote    string
		local     string
		isDir     bool
		blockSize int
	}
	result struct {
		total   int64
		success int64
		failed  int64
	}
}

var up = uploader{}

var uploadCmd = &cobra.Command{
	Use:     "upload",
	Aliases: []string{"up"},
	Short:   "upload files.",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		bucketName := globalConfig.bucketName
		if bucketName == "" {
			return errors.New("bucketName can't be empty")
		}

		source := ""
		target := ""
		if len(args) < 1 {
			err := errors.New("missing SOURCE")
			return err
		} else if len(args) == 1 {
			source = args[0]
			target = ""
		} else {
			source = args[0]
			target = args[1]
		}
		source, err := filepath.Abs(source)
		if err != nil {
			return err
		}
		if isDir(source) {
			up.config.isDir = true
		}

		op, err := NewObjectPath(bucketName, target, globalConfig.bucketURLTpl)
		if err != nil {
			return err
		}
		target = op.name
		up.config.bucketURL = op.bucketURL
		up.config.remote = target
		up.config.local = source

		//if dw.config.isDir && !strings.HasSuffix(target, "/") {
		//	return fmt.Errorf("%s", "when SOURCE is directory, TARGET must be directory too")
		//}
		up.config.blockSize = globalConfig.blockSize * 1024 * 1024

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		cf := up.config
		fmt.Printf("bucketURL: %s\n", cf.bucketURL)
		fmt.Printf("upload %s ==> %s\n", cf.local, cf.remote)
		up.client = cos.NewClient(
			&cos.BaseURL{BucketURL: cf.bucketURL},
			&http.Client{
				Transport: authTransport,
				Timeout:   time.Second * time.Duration(globalConfig.timeout),
			},
		)
		ctx := context.Background()

		start := time.Now()
		up.upload(ctx, cf.local, cf.remote, cf.isDir)
		log.Printf("spend: %d seconds\n", time.Since(start)/time.Second)
		log.Printf("total: %d\n", up.result.total)
		log.Printf("success: %d\n", up.result.success)
		log.Printf("failed: %d\n", up.result.failed)

		if up.result.success != up.result.total {
			log.Printf("upload %s failed", cf.local)
			os.Exit(1)
		}
		return

	},
}

func init() {
	RootCmd.AddCommand(uploadCmd)
}

func (up *uploader) upload(ctx context.Context, localPath, remotePath string, isDir bool) (err error) {
	ws := sync.WaitGroup{}
	err = filepath.Walk(localPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		atomic.AddInt64(&up.result.total, 1)
		ws.Add(1)
		dirName := localPath
		if !isDir {
			dirName = filepath.Dir(localPath)
		}
		rp := remotePath
		if rp == "" || strings.HasSuffix(rp, "/") {
			p := strings.TrimLeft(strings.SplitN(path, dirName, 2)[1],
				string(os.PathSeparator))
			rp = rp + p
		}
		rp = cleanCosPath(rp)
		go func(l, r string) {
			defer ws.Done()
			step := fmt.Sprintf("upload %s -> %s", l, r)
			log.Printf("%s start...", step)
			e := up.uploadFile(ctx, l, r, int(info.Size()))
			if e != nil {
				atomic.AddInt64(&up.result.failed, 1)
				log.Printf("%s failed: %s", step, e)
				return
			}
			atomic.AddInt64(&up.result.success, 1)
			log.Printf("%s success", step)
		}(path, rp)
		return err
	})
	ws.Wait()
	return err
}

func (up *uploader) uploadFile(ctx context.Context, localPath, remotePath string, size int) (err error) {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	if size <= up.config.blockSize {
		return up.uploadFileWhole(ctx, f, remotePath, size)
	} else {
		return up.uploadFileBlocks(ctx, f, remotePath, size)
	}
}

func (up *uploader) uploadFileWhole(ctx context.Context, f *os.File, remotePath string, size int) (err error) {
	opt := &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentLength: size,
		},
	}
	_, err = up.client.Object.Put(ctx, remotePath, f, opt)
	return err
}

func (up *uploader) uploadFileBlocks(ctx context.Context, f *os.File, remotePath string, size int) (err error) {
	fileName := f.Name()
	step0 := fmt.Sprintf("blocks upload %s -> %s", fileName, remotePath)
	log.Printf("%s start...", step0)
	ret, _, err := up.client.Object.InitiateMultipartUpload(ctx, remotePath, nil)
	if err != nil {
		log.Printf("%s failed: %s", step0, err)
		return err
	}
	uploadID := ret.UploadID
	fileSize := size

	ctx, cancel := context.WithCancel(ctx)
	// 分块大小
	bsize := up.config.blockSize
	// 分多少块
	nblock := 1
	if fileSize > bsize {
		nblock = int(math.Ceil(float64(fileSize) / float64(bsize)))
	}
	// 用于保存分块数据
	dataBlock := make(chan struct {
		n    int
		etag string
	}, nblock)
	cerr := make(chan error, nblock)

	step1 := fmt.Sprintf("%s with %d blocks", step0, nblock)
	log.Printf("%s start...", step1)
	// 分块上传
	for i := 1; i <= nblock; i++ {
		s := bsize
		if bsize*i > fileSize {
			s = fileSize - (bsize * (i - 1))
		}
		b := make([]byte, s, s)
		_, err := io.ReadFull(f, b)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("read the %d block of %s failed: %s", i, fileName, err)
			cancel()
			cerr <- err
			break
		}
		block := bytes.NewReader(b)
		go func(n int, block io.Reader) {
			step := fmt.Sprintf("upload the %d block of %s -> %s", n, fileName, remotePath)
			log.Printf("%s start...", step)
			etag, err := up.uploadFileBlock(ctx, block, uploadID, n, remotePath)
			if err != nil {
				log.Printf("%s error: %s", step, err)
				cancel()
				cerr <- err
				return
			}
			log.Printf("%s success", step)
			dataBlock <- struct {
				n    int
				etag string
			}{n, etag}
		}(i, block)
	}

	opt := &cos.CompleteMultipartUploadOptions{}
	for i := 0; i < nblock; i++ {
		select {
		case e := <-cerr:
			return e
		case d := <-dataBlock:
			opt.Parts = append(opt.Parts, cos.Object{
				PartNumber: d.n,
				ETag:       d.etag,
			})
		}
	}
	log.Printf("%s success", step1)
	// 按 PartNumber 排序
	op := objectParts(opt.Parts)
	sort.Sort(op)

	step2 := fmt.Sprintf("%s, complete blocks", step0)
	log.Printf("%s start...", step2)
	_, _, err = up.client.Object.CompleteMultipartUpload(ctx, remotePath, uploadID, opt)

	if err != nil {
		log.Printf("%s failed: %s", step2, err)
		log.Printf("%s failed: %s", step0, err)
		return
	}
	log.Printf("%s success", step2)
	log.Printf("%s success", step0)
	return
}

func (up *uploader) uploadFileBlock(ctx context.Context, f io.Reader, uploadID string,
	partNumber int, remotePath string) (etag string, err error) {
	resp, err := up.client.Object.UploadPart(
		context.Background(), remotePath, uploadID, partNumber, f, nil,
	)
	if err != nil {
		return
	}
	etag = resp.Header.Get("Etag")
	return
}

type objectParts []cos.Object

func (op objectParts) Len() int {
	return len(op)
}
func (op objectParts) Less(i, j int) bool {
	return op[i].PartNumber < op[j].PartNumber
}
func (op objectParts) Swap(i, j int) {
	op[i], op[j] = op[j], op[i]
}
