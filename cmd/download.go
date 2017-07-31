package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mozillazg/go-cos"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
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
	err    chan error
	pb     *mpb.Progress
}
type blockData struct {
	n    int
	path string
}

var dw = &downloader{
	err: make(chan error, 1),
}

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
		defer func() {
			os.RemoveAll(tmpDir)
		}()

		cf := dw.config
		dw.logger = log.WithFields(logrus.Fields{
			"prefix": "download",
		})
		log := dw.logger
		log.Infof("bucketURL: %s", cf.bucketURL)
		log.Infof("download %s ==> %s", cf.remote, cf.local)
		dw.client = cos.NewClient(
			&cos.BaseURL{BucketURL: cf.bucketURL},
			&http.Client{
				Transport: authTransport,
				//Timeout:   time.Second * time.Duration(globalConfig.timeout),
			},
		)
		dw.pb = mpb.New(mpb.WithWidth(64))
		ctx := context.Background()

		start := time.Now()
		dw.download(ctx, cf.remote, cf.local, globalConfig.maxKeys)
		dw.pb.Stop()

		log.Infof("spend: %d seconds", time.Since(start)/time.Second)
		log.Infof("total: %d", dw.result.total)
		log.Infof("success: %d", dw.result.success)
		log.Infof("failed: %d", dw.result.failed)

		errs := []string{}
		close(dw.err)
	loop:
		for {
			select {
			case err, ok := <-dw.err:
				if !ok {
					break loop
				}
				errs = append(errs, fmt.Sprint(err))
			}
		}
		for _, e := range errs {
			log.Error(e)
		}

		if dw.result.success != dw.result.total {
			log.Errorf("download %s failed", cf.remote)
			os.Exit(1)
		}
		return
	},
}

func init() {
	RootCmd.AddCommand(downloadCmd)
}

// 下载文件或目录
func (dw *downloader) download(ctx context.Context, remotePath, localPath string, maxKeys int) (err error) {
	cObjs := make(chan cos.Object, 1)
	cErrs := make(chan error, 1)
	if dw.config.isDir {
		go getObjectsByPrefix(ctx, dw.client, remotePath, maxKeys, cObjs, cErrs)
	} else {
		cObjs <- cos.Object{
			Key: remotePath,
		}
		close(cObjs)
	}

loop:
	for {
		select {
		case o, ok := <-cObjs:
			if !ok {
				break loop
			}
			rp := o.Key
			// 不能下载目录
			if strings.HasSuffix(rp, "/") {
				continue
			}

			atomic.AddInt64(&dw.result.total, 1)
			size := o.Size
			rbase := ""
			if dw.config.isDir {
				rbase = remotePath
			}

			// 提交到消费者队列
			grPool.submit(func() {
				if err = dw.downloadFile(ctx, rbase, rp, localPath, size); err != nil {
					dw.err <- err
					atomic.AddInt64(&dw.result.failed, 1)
				} else {
					atomic.AddInt64(&dw.result.success, 1)
				}
			})
		case err, _ = <-cErrs:
			break loop
		}
	}
	// 等待消费完成
	grPool.join()
	if err != nil {
		dw.err <- err
	}
	return
}

// 下载文件
func (dw *downloader) downloadFile(ctx context.Context,
	remoteBase, remotePath, localPath string, fileSize int) (err error) {
	log := dw.logger
	localPath = getLocalFilePath(remoteBase, remotePath, localPath)
	if err := mkPathDir(localPath); err != nil {
		err = fmt.Errorf("mkdir directory for %s failed: %s", localPath, err)
		log.Error(err)
		return err
	}
	log.Debugf("download %s ==> %s start...", remotePath, localPath)
	blockSize := dw.config.blockSize

	// 获取文件大小
	if fileSize == 0 {
		log.Debugf("get size of %s start...", remotePath)
		fileSize, err = dw.getFileSize(ctx, remotePath)
		if err != nil {
			log.Errorf("get size of %s failed: %s", remotePath, err)
			return err
		}
		log.Debugf("get size of %s success: %d", remotePath, fileSize)
	}

	// 分块并行下载
	err = dw.downloadBlocks(ctx, remotePath, localPath, blockSize, fileSize)
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
	// 分多少块
	nblock := calBlock(fileSize, blockSize)
	// 用于保存分块数据
	dataBlocks := make([]blockData, nblock)
	ret, cerr := make(chan blockData, nblock), make(chan error, nblock)
	gp := grPool.clone()

	step0 := fmt.Sprintf("download %s ==> %s with %d blocks", remotePath, localPath, nblock)
	log.Debugf("%s start...", step0)

	padding := 18
	bar := dw.pb.AddBar(int64(fileSize),
		mpb.PrependDecorators(
			decor.StaticName(remotePath, 0, 0),
			countersDecorator(padding),
		),
		mpb.AppendDecorators(
			speedDecorator(),
			decor.Elapsed(5, decor.DSyncSpace),
		),
	)

	// 分块下载
	for i := 0; i < nblock; i++ {
		n := i
		gp.submit(func() {
			blockPath := fmt.Sprintf("%s.%d", localPathTmp, n)
			_, err := dw.downloadBlock(ctx, remotePath, blockPath, n, blockSize, bar)
			if err != nil {
				log.Errorf("download %s failed: %s", remotePath, err)
				cancel()
				cerr <- err
				return
			}
			ret <- blockData{
				n:    n,
				path: blockPath,
			}
		})
	}
	for i := 0; i < nblock; i++ {
		select {
		case d := <-ret:
			dataBlocks[d.n] = d
		case e := <-cerr:
			return e
		}
	}
	gp.join()
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
	log.Debugf("%s success", step0)
	return
}

// 合并下载的分块
func (dw *downloader) mergeBlocks(localPathTmp string, dataBlocks []blockData) error {
	log := dw.logger
	log.Debugf("merge blocks of %s start...", localPathTmp)
	distFile, err := os.OpenFile(localPathTmp, os.O_CREATE|os.O_APPEND|os.O_WRONLY|os.O_TRUNC, fileMode)
	if err != nil {
		log.Errorf("create file %s failed: %s", localPathTmp, err)
		return err
	}
	defer func() {
		distFile.Sync()
		distFile.Close()
	}()

	// 按顺序合并下载的各个分块文件
	for _, block := range dataBlocks {
		// 打开分块文件
		p := block.path
		f, err := os.OpenFile(p, os.O_RDONLY, fileMode)
		if err != nil {
			log.Errorf("open file %s failed: %s", p, err)
			return err
		}
		// 合并文件内容
		if _, err = io.Copy(distFile, f); err != nil {
			log.Errorf("merge blocks %s of %s failed: %s", localPathTmp, p, err)
			f.Close()
			return err
		}
		f.Close()
		os.Remove(p)
	}
	log.Debugf("merge blocks of %s success", localPathTmp)
	return nil
}

// 下载文件的某块数据
func (dw *downloader) downloadBlock(ctx context.Context,
	remotePath, localPath string, n, bsize int, bar *mpb.Bar) (total int64, err error) {
	client := dw.client
	log := dw.logger
	step := fmt.Sprintf("download the %d block of %s ==> %s", n, remotePath, localPath)
	log.Debugf("%s start...", step)

	// 通过 range 分块下载
	opt := &cos.ObjectGetOptions{
		Range: calRange(bsize, n),
	}
	resp, err := client.Object.Get(ctx, remotePath, opt)
	if err != nil {
		log.Errorf("%s failed: %s", step, err)
		return
	}
	total = resp.ContentLength
	defer resp.Body.Close()

	// 保存 body 内容
	file, err := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fileMode)
	if err != nil {
		log.Errorf("create file %s failed: %s", localPath, err)
		return
	}
	defer func() {
		file.Sync()
		file.Close()
	}()
	writer := bar.ProxyReader(resp.Body)
	written, err := io.Copy(file, writer)
	if err != nil {
		log.Errorf("%s failed: %s", step, err)
		return
	}
	// 判断 body 内容是否读取完全
	if written != total {
		err = errors.New(
			fmt.Sprintf("%s failed: written(%d) != total(%d)",
				step, written, total),
		)
	}
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
func getLocalFilePath(remoteBase, remotePath, localPath string) string {
	if localPath == "" {
		localPath = getFileName(remotePath)
	}

	if remoteBase == "" {
		remotePath = getFileName(remotePath)
	} else { // 除去 remoteBase 外的文件路径
		remotePath = strings.SplitN(remotePath, remoteBase, 2)[1]
	}

	// 按原有目录结构保存到本地
	if isDir(localPath) || strings.HasSuffix(localPath, "/") {
		localPath = filepath.Join([]string{
			localPath, filepath.Dir(remotePath), getFileName(remotePath),
		}...)
	}
	return localPath
}
