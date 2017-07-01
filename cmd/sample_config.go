package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

const sampleConfig = `secret_id: ""
secret_key: ""
base_url:
  # e.g. "https://{{.BucketName}}-1253846586.cn-north.myqcloud.com"
  bucket: "https://{{.BucketName}}-AppID.Region.myqcloud.com"
  service: "https://service.cos.myqcloud.com"

concurrency: 30
timeout: 180       # second
verbose: false
block_size: 50   # MB
bucket_name: ""
`

// sampleConfigCmd represents the sampleConfig command
var sampleConfigCmd = &cobra.Command{
	Use:   "sampleConfig",
	Short: "Echo sample yaml config.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Print(sampleConfig)
	},
}

func init() {
	RootCmd.AddCommand(sampleConfigCmd)
}
