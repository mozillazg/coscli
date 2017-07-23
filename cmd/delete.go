package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"log"
	"sync"

	"github.com/mozillazg/go-cos"
	"github.com/spf13/cobra"
)

var deleteConfig = struct {
	bucketURL *url.URL
	prefix    string
	maxKeys   int
}{}

var deleteCmd = &cobra.Command{
	Use:     "delete",
	Aliases: []string{"del", "rm", "remove"},
	Short:   "delete Objects by prefix",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		bucketName := globalConfig.bucketName
		if bucketName == "" {
			return errors.New("bucketName can't be empty")
		}
		prefix := ""
		if len(args) > 0 {
			prefix = args[0]
		}
		op, err := NewObjectPath(bucketName, prefix, globalConfig.bucketURLTpl)
		if err != nil {
			return err
		}

		deleteConfig.prefix = op.name
		deleteConfig.bucketURL = op.bucketURL
		deleteConfig.maxKeys = globalConfig.maxKeys
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("bucketURL: %s\n", deleteConfig.bucketURL)
		fmt.Printf("prefix: %s\n", deleteConfig.prefix)
		client := cos.NewClient(
			&cos.BaseURL{BucketURL: deleteConfig.bucketURL},
			&http.Client{
				Transport: authTransport,
				Timeout:   time.Second * time.Duration(globalConfig.timeout),
			},
		)
		ctx := context.Background()
		err := deleteObjects(ctx, client, deleteConfig.prefix, deleteConfig.maxKeys)
		if err != nil {
			exitWithError(err)
		}

	},
}

func init() {
	RootCmd.AddCommand(deleteCmd)
}

func deleteObjects(ctx context.Context, client *cos.Client, prefix string, maxKeys int) error {
	next := ""
	wg := sync.WaitGroup{}
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
		wg.Add(1)
		go func(rt *cos.BucketGetResult) {
			defer wg.Done()
			e := deleteObject(ctx, client, rt.Contents)
			if e != nil {
				log.Printf("delete objects failed: %s", e)
			}
		}(ret)
		next = ret.NextMarker
		if next == "" {
			break
		}
	}
	wg.Wait()
	return nil
}

func deleteObject(ctx context.Context, client *cos.Client, objects []cos.Object) error {
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
		log.Printf("delete %s success", o.Key)
	}
	for _, e := range ret.Errors {
		log.Printf("delete %s failed: %s", e.Key, e.Message)
	}
	return err
}
