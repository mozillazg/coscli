package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mozillazg/go-cos"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
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
		total   int64
		success int64
		failed  int64
	}
	logger *logrus.Entry
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
	Use:     "download SOURCE [TARGET]",
	Aliases: []string{"down", "dw", "get"},
	Short:   "download files",
	Long: `download fies.

examples:

  download path/to/object
  download path/to/object  /path/to/local_file
  download path/to/object  /path/to/local_dir/
  download path/to/dir/  /path/to/local_dir/
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
				Timeout:   time.Second * time.Duration(globalConfig.timeout),
			},
		)
		dw.logger = log.WithFields(logrus.Fields{
			"prefix": "download",
		})
		log := dw.logger
		ctx := context.Background()
		defer func() {
			os.RemoveAll(tmpDir)
		}()

		if dw.config.isDir {
			start := time.Now()
			dw.downloadDir(ctx, cf.remote, cf.local, globalConfig.maxKeys)
			log.Infof("spend: %d seconds", time.Since(start)/time.Second)
			log.Infof("total: %d", dw.result.total)
			log.Infof("success: %d", dw.result.success)
			log.Infof("failed: %d", dw.result.failed)

			if dw.result.success != dw.result.total {
				log.Errorf("download %s failed", cf.remote)
				os.Exit(1)
			}
			return
		}
		if err = dw.downloadFile(ctx, cf.remote, cf.local, 0); err != nil {
			log.Errorf("download %s failed: %s", cf.remote, err)
			os.Exit(1)
		}
		return
	},
}

func init() {
	RootCmd.AddCommand(downloadCmd)
}

// 下载文件夹
func (dw *downloader) downloadDir(ctx context.Context, remoteDir, localDir string, maxKeys int) (err error) {
	next := ""
	var wg sync.WaitGroup
	for {
		opt := &cos.BucketGetOptions{
			Prefix:  remoteDir,
			MaxKeys: maxKeys,
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
			lp := getLocalDirPath(remoteDir, rp, localDir)
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
	remotePath, localPath string, fileSize int) (err error) {
	log := dw.logger
	localPath = getLocalFilePath(remotePath, localPath)
	if err := mkPathDir(localPath); err != nil {
		err = fmt.Errorf("mkdir directory for %s failed: %s", localPath, err)
		log.Error(err)
		return err
	}
	log.Infof("download %s ==> %s start...", remotePath, localPath)
	blockSize := dw.config.blockSize

	if fileSize == 0 {
		log.Debugf("get size of %s start...", remotePath)
		// 获取文件大小
		fileSize, err = dw.getFileSize(ctx, remotePath)
		if err != nil {
			log.Errorf("get size of %s failed: %s", remotePath, err)
			return err
		}
		log.Debugf("get size of %s success: %d", remotePath, fileSize)
	}
	if fileSize <= blockSize {
		return dw.downloadWhole(ctx, remotePath, localPath)
	}

	// 分块并行下载
	return dw.downloadBlocks(ctx, remotePath, localPath, blockSize, fileSize)
}

// 下载整个文件，不分块下载
func (dw *downloader) downloadWhole(ctx context.Context, remotePath, localPath string) (err error) {
	log := dw.logger
	log.Infof("download %s ==> %s start...", remotePath, localPath)
	localPathTmp := getTmpFilePath(localPath, tmpDir)
	if err := mkPathDir(localPathTmp); err != nil {
		err = fmt.Errorf("mkdir directory for %s failed: %s", localPath, err)
		log.Error(err)
		return err
	}
	client := dw.client
	step0 := fmt.Sprintf("download %s ==> %s", remotePath, localPathTmp)
	log.Debugf("%s start...", step0)

	// 下载文件
	resp, err := client.Object.Get(ctx, remotePath, nil)
	if err != nil {
		log.Errorf("%s failed: %s", step0, err)
		return
	}
	defer resp.Body.Close()

	// 把下载的数据保存到临时文件中
	file, err := os.OpenFile(localPathTmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, fileMode)
	if err != nil {
		log.Errorf("create file %s failed: %s", localPathTmp, err)
		return
	}
	defer file.Close()
	io.Copy(file, resp.Body)
	log.Debugf("%s success", step0)

	// 生成最终的文件
	step2 := fmt.Sprintf("rename %s to %s", localPathTmp, localPath)
	log.Debugf("%s start...", step2)
	if err = os.Rename(localPathTmp, localPath); err != nil {
		log.Errorf("%s failed: %s", step2, err)
		return err
	}
	log.Debugf("%s success", step2)
	log.Infof("download %s ==> %s success", remotePath, localPath)
	return
}

// 分块下载文件
func (dw *downloader) downloadBlocks(ctx context.Context, remotePath, localPath string,
	blockSize, fileSize int) (err error) {
	log := dw.logger
	localPathTmp := getTmpFilePath(localPath, tmpDir)
	if err := mkPathDir(localPathTmp); err != nil {
		err = fmt.Errorf("mkdir directory for %s failed: %s", localPath, err)
		log.Error(err)
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	// 分块大小
	bsize := float64(blockSize)
	// 分多少块
	nblock := 1
	if fileSize > int(bsize) {
		nblock = int(math.Ceil(float64(fileSize) / bsize))
	}
	// 用于保存分块数据
	dataBlocks := make([]blockData, nblock)
	ret, cerr := make(chan blockData, nblock), make(chan error, nblock)

	step0 := fmt.Sprintf("download %s ==> %s with %d blocks", remotePath, localPath, nblock)
	log.Debugf("%s start...", step0)
	// 分块下载
	for i := 0; i < nblock; i++ {
		go func(n int) {
			blockPath := fmt.Sprintf("%s.%d", localPathTmp, n)
			f, err := dw.downloadBlock(ctx, remotePath, blockPath, n, int(bsize))
			if err != nil {
				log.Errorf("download %s failed: %s", remotePath, err)
				cancel()
				cerr <- err
				return
			}
			ret <- blockData{
				n:    n,
				path: blockPath,
				f:    f,
			}
		}(i)
	}
	for i := 0; i < nblock; i++ {
		select {
		case d := <-ret:
			dataBlocks[d.n] = d
		case e := <-cerr:
			return e
		}
	}
	// 合并分块
	if err = dw.mergeBlocks(localPathTmp, dataBlocks); err != nil {
		return
	}
	// 生成最终的文件
	step2 := fmt.Sprintf("rename %s to %s", localPathTmp, localPath)
	log.Debugf("%s start...", step2)
	if err = os.Rename(localPathTmp, localPath); err != nil {
		log.Errorf("%s failed: %s", step2, err)
		return err
	}
	log.Debugf("%s success", step2)
	log.Infof("%s success", step0)
	return
}

// 合并下载的分块
func (dw *downloader) mergeBlocks(localPathTmp string, dataBlocks []blockData) error {
	defer func() {
		for _, block := range dataBlocks {
			block.f.Close()
		}
	}()
	log := dw.logger

	log.Debugf("merge blocks of %s start...", localPathTmp)
	file, err := os.OpenFile(localPathTmp, os.O_CREATE|os.O_APPEND|os.O_WRONLY|os.O_TRUNC, fileMode)
	if err != nil {
		log.Errorf("create file %s failed: %s", localPathTmp, err)
		return err
	}

	for _, block := range dataBlocks {
		f := block.f
		p := block.path
		f.Seek(io.SeekStart, io.SeekStart)
		if _, err = io.Copy(file, f); err != nil {
			log.Errorf("merge blocks %s of %s failed: %s", localPathTmp, p, err)
			return err
		}
	}
	log.Debugf("merge blocks of %s success", localPathTmp)
	return nil
}

// 下载文件的某块数据
func (dw *downloader) downloadBlock(ctx context.Context,
	remotePath, localPath string, n, bsize int) (f *os.File, err error) {
	client := dw.client
	log := dw.logger
	step := fmt.Sprintf("download %d block of %s ==> %s", n, remotePath, localPath)
	log.Debugf("%s start...", step)

	start := bsize * n
	end := bsize*(n+1) - 1
	rg := fmt.Sprintf("bytes=%d-%d", start, end)
	opt := &cos.ObjectGetOptions{
		Range: rg,
	}
	resp, err := client.Object.Get(ctx, remotePath, opt)
	if err != nil {
		log.Errorf("%s failed: %s", step, err)
		return nil, err
	}
	defer resp.Body.Close()

	file, err := os.OpenFile(localPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, fileMode)
	if err != nil {
		log.Errorf("create file %s failed: %s", localPath, err)
		return nil, err
	}
	io.Copy(file, resp.Body)
	f = file
	log.Debugf("%s success", step)
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
func getLocalDirPath(remoteBase, remotePath, localDir string) string {
	remotePath = strings.SplitN(remotePath, remoteBase, 2)[1]
	return filepath.Join([]string{
		localDir, filepath.Dir(remotePath),
	}...) + string(os.PathSeparator)
}

//
func getLocalFilePath(remotePath, localPath string) string {
	if localPath == "" {
		localPath = getFileName(remotePath)
	}

	if isDir(localPath) || strings.HasSuffix(localPath, "/") {
		localPath = filepath.Join([]string{localPath, getFileName(remotePath)}...)
	}
	return localPath
}
