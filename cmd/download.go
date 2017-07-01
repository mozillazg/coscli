package cmd

import (
	"fmt"

	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"

	"strings"

	"math"
	"path/filepath"
	"sync"

	"github.com/mozillazg/go-cos"
	"github.com/spf13/cobra"
	"sync/atomic"
	"time"
)

type downloader struct {
	client *cos.Client
	config struct {
		bucketURL *url.URL
		remote    string
		local     string
		isDir     bool
		blockSize int
	}
	result struct {
		total int64
		success int64
		failed int64
	}
}
type blockData struct {
	n    int
	path string
	f    *os.File
}

var dw = new(downloader)

const tmpDir = ".coscli_tmp"

// downloadCmd represents the download command
var downloadCmd = &cobra.Command{
	Use:   "download SOURCE [TARGET]",
	Aliases: []string{"down", "dw"},
	Short: "download files",
	Long: `download fies.

examples:

  download /bucketName/path/to/object
  download /bucketName/path/to/object  /path/to/local_file
  download /bucketName/path/to/object  /path/to/local_dir/
  download /bucketName/path/to/dir/  /path/to/local_dir/
  download -b bucketName /path/to/object
	`,
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
			target = "."
		} else {
			source = args[0]
			target = args[1]
		}

		op, err := NewObjectPath(bucketName, source, globalConfig.bucketURLTpl)
		if err != nil {
			return err
		}
		source = op.name
		if strings.HasSuffix(source, "/") {
			dw.config.isDir = true
		}
		dw.config.bucketURL = op.bucketURL
		dw.config.remote = source
		dw.config.local = target

		// 创建临时目录
		if err := os.MkdirAll(tmpDir, dirMode); err != nil {
			return fmt.Errorf("mkdir temp directory %s failed: %s", tmpDir, err)
		}

		// 创建保存文件的目录
		if strings.HasSuffix(target, string(os.PathSeparator)) {
			if err := mkPathDir(target); err != nil {
				return fmt.Errorf("mkdir directory %s failed: %s", target, err)
			}
		}
		if dw.config.isDir && !isDir(target) {
			return fmt.Errorf("%s", "when SOURCE is directory, TARGET must be directory too")
		}
		dw.config.blockSize = globalConfig.blockSize * 1024 * 1024

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		cf := dw.config
		var err error
		fmt.Printf("bucketURL: %s\n", cf.bucketURL)
		fmt.Printf("download %s ==> %s\n", cf.remote, cf.local)
		dw.client = cos.NewClient(
			&cos.BaseURL{BucketURL: cf.bucketURL},
			&http.Client{
				Transport: authTransport,
				Timeout: time.Second * time.Duration(globalConfig.timeout),
			},
		)
		ctx := context.Background()
		defer func() {
			os.RemoveAll(tmpDir)
		}()

		if dw.config.isDir {
			start := time.Now()
			dw.downloadDir(ctx, cf.remote, cf.local)
			log.Printf("spend: %d seconds\n", time.Since(start)/time.Second)
			log.Printf("total: %d\n", dw.result.total)
			log.Printf("success: %d\n", dw.result.success)
			log.Printf("failed: %d\n", dw.result.failed)

			if dw.result.success != dw.result.total {
				log.Printf("download %s failed", cf.remote)
				os.Exit(1)
			}
			return
		}
		if err = dw.downloadFile(ctx, cf.remote, cf.local, 0); err != nil {
			log.Printf("download %s failed: %s", cf.remote, err)
			os.Exit(1)
		}
		return
	},
}

func init() {
	RootCmd.AddCommand(downloadCmd)
}

// 下载文件夹
func (dw *downloader) downloadDir(ctx context.Context, remote_dir, local_dir string) (err error) {
	next := ""
	var wg sync.WaitGroup
	for {
		opt := &cos.BucketGetOptions{
			Prefix:  remote_dir,
			MaxKeys: 10,
		}
		if next != "" {
			opt.Marker = next
		}
		ret, _, err := dw.client.Bucket.Get(ctx, opt)
		if err != nil {
			return err
		}
		next = ret.NextMarker
		for _, o := range ret.Contents {
			rp := o.Key
			if strings.HasSuffix(rp, "/") {
				continue
			}
			wg.Add(1)
			atomic.AddInt64(&dw.result.total, 1)
			size := o.Size
			lp := getLocalDirPath(remote_dir, rp, local_dir)
			go func(rp, lp string, size int) {
				defer wg.Done()
				if err = dw.downloadFile(ctx, rp, lp, size); err != nil {
					atomic.AddInt64(&dw.result.failed, 1)
				} else {
					atomic.AddInt64(&dw.result.success, 1)
				}
			}(rp, lp, size)
		}
		if next == "" {
			break
		}
	}
	wg.Wait()
	return
}

// 下载文件
func (dw *downloader) downloadFile(ctx context.Context,
	remote_path, local_path string, fileSize int) (err error) {
	local_path = getLocalFilePath(remote_path, local_path)
	if err := mkPathDir(local_path); err != nil {
		err = fmt.Errorf("mkdir directory for %s failed: %s", local_path, err)
		log.Println(err)
		return err
	}
	log.Printf("download %s ==> %s start...", remote_path, local_path)
	blockSize := dw.config.blockSize

	if fileSize == 0 {
		log.Printf("get size of %s start...", remote_path)
		// 获取文件大小
		fileSize, err = dw.getFileSize(ctx, remote_path)
		if err != nil {
			log.Printf("get size of %s failed: %d", remote_path, err)
			return err
		}
		log.Printf("get size of %s success: %s", remote_path, fileSize)
	}
	if fileSize <= blockSize {
		return dw.downloadWhole(ctx, remote_path, local_path)
	}

	// 分块并行下载
	return dw.downloadBlocks(ctx, remote_path, local_path, blockSize, fileSize)
}

// 下载整个文件，不分块下载
func (dw *downloader) downloadWhole(ctx context.Context, remote_path, local_path string) (err error) {
	log.Printf("download %s ==> %s start...", remote_path, local_path)
	local_path_tmp := getTmpFilePath(local_path, tmpDir)
	if err := mkPathDir(local_path_tmp); err != nil {
		err = fmt.Errorf("mkdir directory for %s failed: %s", local_path, err)
		log.Println(err)
		return err
	}
	client := dw.client
	step0 := fmt.Sprintf("download %s ==> %s", remote_path, local_path_tmp)
	log.Printf("%s start...", step0)

	// 下载文件
	resp, err := client.Object.Get(ctx, remote_path, nil)
	if err != nil {
		log.Printf("%s failed: %s", step0, err)
		return
	}
	defer resp.Body.Close()

	// 把下载的数据保存到临时文件中
	file, err := os.OpenFile(local_path_tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, fileMode)
	if err != nil {
		log.Printf("create file %s failed: %s", local_path_tmp, err)
		return
	}
	defer file.Close()
	io.Copy(file, resp.Body)
	log.Printf("%s success", step0)

	// 生成最终的文件
	step2 := fmt.Sprintf("rename %s to %s", local_path_tmp, local_path)
	log.Printf("%s start...", step2)
	if err = os.Rename(local_path_tmp, local_path); err != nil {
		log.Printf("%s failed: %s", step2, err)
		return err
	}
	log.Printf("%s success", step2)
	log.Printf("download %s ==> %s success", remote_path, local_path)
	return
}

// 分块下载文件
func (dw *downloader) downloadBlocks(ctx context.Context, remote_path, local_path string,
	blockSize, fileSize int) (err error) {
	local_path_tmp := getTmpFilePath(local_path, tmpDir)
	if err := mkPathDir(local_path_tmp); err != nil {
		err = fmt.Errorf("mkdir directory for %s failed: %s", local_path, err)
		log.Println(err)
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	// 分块大小
	bsize := float64(blockSize * 1024 * 1024)
	// 分多少块
	nblock := 1
	if fileSize > int(bsize) {
		nblock = int(math.Ceil(float64(fileSize) / bsize))
	}
	// 用于保存分块数据
	dataBlocks := make([]blockData, nblock)
	ret, cerr := make(chan blockData, nblock), make(chan error, nblock)

	step0 := fmt.Sprintf("download %s ==> %s with %d blocks", remote_path, local_path, nblock)
	log.Printf("%s start...", step0)
	// 分块下载
	for i := 0; i < nblock; i++ {
		go func(n int) {
			block_path := fmt.Sprintf("%s.%d", local_path_tmp, n)
			f, err := dw.downloadBlock(ctx, remote_path, block_path, n, int(bsize))
			if err != nil {
				log.Printf("download %s failed: %s", remote_path, err)
				cancel()
				cerr <- err
				return
			}
			ret <- blockData{
				n:    n,
				path: block_path,
				f:    f,
			}
		}(i)
	}
	for i := 0; i < nblock; i++ {
		select {
		case d := <-ret:
			dataBlocks[d.n] = d
		case e := <- cerr:
			return e
		}
	}
	// 合并分块
	if err = dw.mergeBlocks(local_path_tmp, dataBlocks); err != nil {
		return
	}
	// 生成最终的文件
	step2 := fmt.Sprintf("rename %s to %s", local_path_tmp, local_path)
	log.Printf("%s start...", step2)
	if err = os.Rename(local_path_tmp, local_path); err != nil {
		log.Printf("%s failed: %s", step2, err)
		return err
	}
	log.Printf("%s success", step2)
	log.Printf("%s success", step0)
	return
}

// 合并下载的分块
func (dw *downloader) mergeBlocks(local_path_tmp string, dataBlocks []blockData) error {
	defer func() {
		for _, block := range dataBlocks {
			block.f.Close()
		}
	}()

	log.Printf("merge blocks of %s start...", local_path_tmp)
	file, err := os.OpenFile(local_path_tmp, os.O_CREATE|os.O_APPEND|os.O_WRONLY|os.O_TRUNC, fileMode)
	if err != nil {
		log.Printf("create file %s failed: %s", local_path_tmp, err)
		return err
	}

	for _, block := range dataBlocks {
		f := block.f
		p := block.path
		f.Seek(io.SeekStart, io.SeekStart)
		if _, err = io.Copy(file, f); err != nil {
			log.Printf("merge blocks %s of %s failed: %s", local_path_tmp, p, err)
			return err
		}
	}
	log.Printf("merge blocks of %s success", local_path_tmp)
	return nil
}

// 下载文件的某块数据
func (dw *downloader) downloadBlock(ctx context.Context,
	remote_path, local_path string, n, bsize int) (f *os.File, err error) {
	client := dw.client
	step := fmt.Sprintf("download %d block of %s ==> %s", n, remote_path, local_path)
	log.Printf("%s start...", step)

	start := bsize * n
	end := bsize*(n+1) - 1
	rg := fmt.Sprintf("bytes=%d-%d", start, end)
	opt := &cos.ObjectGetOptions{
		Range: rg,
	}
	resp, err := client.Object.Get(ctx, remote_path, opt)
	if err != nil {
		log.Printf("%s failed: %s", step, err)
		return nil, err
	}
	defer resp.Body.Close()

	file, err := os.OpenFile(local_path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, fileMode)
	if err != nil {
		log.Printf("create file %s failed: %s", local_path, err)
		return nil, err
	}
	io.Copy(file, resp.Body)
	f = file
	log.Printf("%s success", step)
	return
}

// 获取文件大小
func (dw *downloader) getFileSize(ctx context.Context, name string) (int, error) {
	resp, err := dw.client.Object.Head(ctx, name, nil)
	if err != nil {
		return 0, err
	}

	return int(resp.ContentLength), nil
}

func getTmpFilePath(path, prefixDir string) string {
	return filepath.Join([]string{
		prefixDir, path,
	}...)
}

// 创建文件所在的目录
func mkPathDir(path string) error {
	dir := filepath.Dir(path)
	if dir != "" {
		return os.MkdirAll(dir, dirMode)
	}
	return nil
}

//
func getLocalDirPath(remote_base, remote_path, local_dir string) string {
	remote_path = strings.SplitN(remote_path, remote_base, 2)[1]
	return filepath.Join([]string{
		local_dir, filepath.Dir(remote_path),
	}...) + string(os.PathSeparator)
}

//
func getLocalFilePath(remote_path, local_path string) string {
	if local_path == "" {
		local_path = getFileName(remote_path)
	}

	if isDir(local_path) || strings.HasSuffix(local_path, "/") {
		local_path = filepath.Join([]string{local_path, getFileName(remote_path)}...)
	}
	return local_path
}
