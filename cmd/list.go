package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/mozillazg/go-cos"
	"github.com/spf13/cobra"
)

var listConfig = struct {
	bucketURL *url.URL
	prefix    string
	maxKeys   int
}{}

var listCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "list Objects by prefix",
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

		listConfig.prefix = op.name
		listConfig.bucketURL = op.bucketURL
		listConfig.maxKeys = globalConfig.maxKeys
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("bucketURL: %s\n", listConfig.bucketURL)
		fmt.Printf("prefix: %s\n", listConfig.prefix)
		client := cos.NewClient(
			&cos.BaseURL{BucketURL: listConfig.bucketURL},
			&http.Client{
				Transport: authTransport,
				Timeout:   time.Second * time.Duration(globalConfig.timeout),
			},
		)
		ctx := context.Background()
		err := listObjects(ctx, client, listConfig.prefix, listConfig.maxKeys)
		if err != nil {
			exitWithError(err)
		}

	},
}

func init() {
	RootCmd.AddCommand(listCmd)
}

func listObjects(ctx context.Context, client *cos.Client, prefix string, maxKeys int) error {
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
			return err
		}
		if len(ret.Contents) == 0 {
			log.Print("nothing")
			break
		}
		formatObjectList(ret.Contents)
		next = ret.NextMarker
		if next == "" {
			break
		}
	}
	return nil
}

func formatObjectList(objects []cos.Object) {
	for _, ob := range objects {
		fmt.Printf("%s  %d  %s  %s\n", ob.Key, ob.Size, ob.LastModified, ob.StorageClass)
	}
	return
}
