package cmd

import (
	"fmt"

	"context"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/mozillazg/go-cos"
	"github.com/spf13/cobra"
	"github.com/sirupsen/logrus"
	"os"
)

var listPartsConfig = struct {
	bucketURL *url.URL
	prefix    string
	maxKeys   int
}{}

var listPartsCmd = &cobra.Command{
	Use:     "list-parts",
	Aliases: []string{"lsp"},
	Short:   "List Multipart Uploads",
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

		listPartsConfig.prefix = op.name
		listPartsConfig.bucketURL = op.bucketURL
		listPartsConfig.maxKeys = globalConfig.maxKeys
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		logger := log.WithFields(logrus.Fields{
			"prefix": "delete-part",
		})
		logger.Infof("bucketURL: %s", listPartsConfig.bucketURL)
		logger.Infof("prefix: %s", listPartsConfig.prefix)

		client := cos.NewClient(
			&cos.BaseURL{BucketURL: listPartsConfig.bucketURL},
			&http.Client{
				Transport: authTransport,
				Timeout:   time.Second * time.Duration(globalConfig.timeout),
			},
		)
		ctx := context.Background()
		step := fmt.Sprint("List Multipart Uploads")
		err := listParts(ctx, client)
		if err != nil {
			logger.Errorf("%s failed: %s", step, err)
			os.Exit(1)
		}
		logger.Infof("%s success", step)
	},
}

func init() {
	RootCmd.AddCommand(listPartsCmd)
}

func listParts(ctx context.Context, client *cos.Client) error {
	next := ""
	for {
		opt := &cos.ListMultipartUploadsOptions{
			MaxUploads: listPartsConfig.maxKeys,
			Prefix:     listPartsConfig.prefix,
		}
		if next != "" {
			opt.KeyMarker = next
		}

		ret, _, err := client.Bucket.ListMultipartUploads(ctx, opt)
		if err != nil {
			return err
		}

		if len(ret.Uploads) == 0 {
			log.Print("nothing")
			break
		}

		formatPartList(ret)

		next = ret.NextKeyMarker
		if next == "" {
			break
		}
	}
	return nil
}

func formatPartList(ret *cos.ListMultipartUploadsResult) {
	uploads := ret.Uploads
	for _, up := range uploads {
		fmt.Printf("%s  %s  %s\n", up.Key, up.UploadID, up.Initiated)
	}
	return
}
