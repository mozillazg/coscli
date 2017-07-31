package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mozillazg/go-cos"
	"github.com/spf13/cobra"
	"github.com/sirupsen/logrus"
)

type objectDeleter struct {
	config struct {
		bucketURL *url.URL
		path      string
		isPrefix  bool
		maxKeys   int
	}
	client *cos.Client
	logger *logrus.Entry
}

var od = new(objectDeleter)
var deleteCmd = &cobra.Command{
	Use:     "delete",
	Aliases: []string{"del", "rm", "remove"},
	Short:   "delete Object by path",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		bucketName := globalConfig.bucketName
		if bucketName == "" {
			return errors.New("bucketName can't be empty")
		}
		prefix := ""
		if len(args) > 0 {
			prefix = args[0]
		}
		if prefix == "" {
			return errors.New("object path can't be empty")
		}
		op, err := NewObjectPath(bucketName, prefix, globalConfig.bucketURLTpl)
		if err != nil {
			return err
		}

		od.config.path = op.name
		od.config.bucketURL = op.bucketURL
		od.config.maxKeys = globalConfig.maxKeys
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		cfg := od.config
		fmt.Printf("bucketURL: %s\n", cfg.bucketURL)
		fmt.Printf("path: %s\n", cfg.path)
		od.client = cos.NewClient(
			&cos.BaseURL{BucketURL: cfg.bucketURL},
			&http.Client{
				Transport: authTransport,
				Timeout:   time.Second * time.Duration(globalConfig.timeout),
			},
		)
		ctx := context.Background()
		var err error
		od.logger = log.WithFields(logrus.Fields{
			"prefix": "delete",
		})
		log := od.logger
		step := fmt.Sprintf("delete %s", cfg.path)
		log.Infof("%s start...", step)
		if cfg.isPrefix {
			err = od.deleteByPrefix(ctx)
		} else {
			err = od.deleteObject(ctx)
		}
		if err != nil {
			log.Errorf("%s failed", step)
			exitWithError(err)
		} else {
			log.Infof("%s success", step)
		}

	},
}

func init() {
	RootCmd.AddCommand(deleteCmd)
	deleteCmd.Flags().BoolVar(&od.config.isPrefix, "prefix", false, "delete objects by prefix")
}

func (od *objectDeleter) deleteByPrefix(ctx context.Context) error {
	next := ""
	prefix := od.config.path
	maxKeys := od.config.maxKeys
	client := od.client
	eMsg := []string{}
	eLock := sync.Mutex{}
	nothing := true
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
			return err
		}
		// 没有符合条件的 objects
		if len(ret.Contents) == 0 {
			break
		} else {
			nothing = false
		}

		// 放到 pool 消费者队列中
		grPool.submit(func() {
			e := od.BatchDelete(ctx, ret.Contents)
			if e != nil {
				eLock.Lock()
				defer eLock.Unlock()
				eMsg = append(eMsg, fmt.Sprint(e))
			}
		})

		next = ret.NextMarker
		// 没有下一页了
		if next == "" {
			break
		}
	}
	// 等待消费者消费完成
	grPool.join()

	if len(eMsg) > 0 {
		return errors.New(strings.Join(eMsg, "\n"))
	}
	if nothing {
		od.logger.Warning("Nothing!")
	}
	return nil
}

func (od *objectDeleter) BatchDelete(ctx context.Context, objects []cos.Object) error {
	client := od.client
	log := od.logger
	obs := []cos.Object{}
	for _, v := range objects {
		obs = append(obs, cos.Object{Key: v.Key})
	}
	opt := &cos.ObjectDeleteMultiOptions{
		Objects: obs,
		Quiet:   false,
	}
	ret, _, err := client.Object.DeleteMulti(ctx, opt)
	if err != nil {
		return err
	}

	for _, o := range ret.DeletedObjects {
		log.Infof("delete %s success", o.Key)
	}
	eMsg := []string{}
	for _, e := range ret.Errors {
		eMsg = append(eMsg, fmt.Sprintf("delete %s failed: %s", e.Key, e.Message))
	}
	if len(eMsg) > 0 {
		return errors.New(strings.Join(eMsg, "\n"))
	}
	return nil
}

func (od *objectDeleter) deleteObject(ctx context.Context) error {
	client := od.client
	path := od.config.path
	_, err := client.Object.Delete(ctx, path)
	if err != nil {
		return err
	}
	return err
}
