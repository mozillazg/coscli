package cmd

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/mozillazg/go-cos"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var listBucketsCmd = &cobra.Command{
	Use:     "list-buckets",
	Aliases: []string{"lb"},
	Short:   "List buckets",
	Run: func(cmd *cobra.Command, args []string) {
		err := listBuckets()
		if err != nil {
			exitWithError(err)
		}
	},
}

func init() {
	RootCmd.AddCommand(listBucketsCmd)
}

func listBuckets() error {
	client := cos.NewClient(nil,
		&http.Client{
			Transport: authTransport,
			Timeout:   time.Second * time.Duration(globalConfig.timeout),
		},
	)
	ctx := context.Background()
	ret, _, err := client.Service.Get(ctx)
	if err != nil {
		return err
	}
	formatBucketList(ret.Buckets)
	return nil
}

func formatBucketList(buckets []cos.Bucket) {
	table := tablewriter.NewWriter(os.Stdout)
	number := len(buckets)
	data := make([][]string, number, number)
	for n, b := range buckets {
		data[n] = []string{
			b.Name, b.Region, b.CreateDate,
		}
	}
	table.SetHeader([]string{"Name", "Region", "CreateDate"})
	table.AppendBulk(data)
	table.Render()
	return
}
