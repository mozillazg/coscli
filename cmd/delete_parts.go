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

var deletePartConfig = struct {
	bucketURL *url.URL
	name string
	uploadID string
}{}

var deletePartCmd = &cobra.Command{
	Use:     "delete-part",
	Aliases: []string{"delp", "rmp"},
	Short:   "abort and delete Multipart Upload",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		bucketName := globalConfig.bucketName
		if bucketName == "" {
			return errors.New("bucketName can't be empty")
		}
		uploadID := ""
		name := ""
		if len(args) > 0 {
			name = args[0]
		}
		if len(args) > 1 {
			uploadID = args[1]
		}
		if uploadID == "" || name == "" {
			return errors.New("name and uploadID can't be empty")
		}
		op, err := NewObjectPath(bucketName, name, globalConfig.bucketURLTpl)
		if err != nil {
			return err
		}

		deletePartConfig.name = op.name
		deletePartConfig.bucketURL = op.bucketURL
		deletePartConfig.uploadID = uploadID
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		logger := log.WithFields(logrus.Fields{
			"prefix": "delete-part",
		})
		logger.Infof("bucketURL: %s", deletePartConfig.bucketURL)
		logger.Infof("name: %s", deletePartConfig.name)
		logger.Infof("uploadID: %s", deletePartConfig.uploadID)

		client := cos.NewClient(
			&cos.BaseURL{BucketURL: deletePartConfig.bucketURL},
			&http.Client{
				Transport: authTransport,
				Timeout:   time.Second * time.Duration(globalConfig.timeout),
			},
		)
		step := fmt.Sprint("abort and delete Multipart Upload")
		ctx := context.Background()
		err := deletePart(ctx, client)
		if err != nil {
			logger.Errorf("%s failed: %s", step, err)
			os.Exit(1)
		}
		logger.Infof("%s success", step)
	},
}

func init() {
	RootCmd.AddCommand(deletePartCmd)
}

func deletePart(ctx context.Context, client *cos.Client) error {
	_, err := client.Object.AbortMultipartUpload(
		ctx, deletePartConfig.name, deletePartConfig.uploadID,
	)
	return err
}
