package cmd

import (
	"fmt"
	"os"

	"net/http"
	"text/template"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/mozillazg/go-cos"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var log = logrus.New()
var cfgFile string
var authTransport http.RoundTripper

const fileMode = 0644
const dirMode = 0755

var globalConfig = struct {
	secretID     string
	secretKey    string
	concurrency  int
	timeout      int
	verbose      bool
	bucketURL    string
	serviceURL   string
	bucketName   string
	blockSize    int // 上传/下载的分块大小 (MB)
	bucketURLTpl *template.Template
	maxKeys      int // download/list 时遍历文件前缀的每次请求响应对象数
}{
	concurrency: 30,
	timeout:     60,
	verbose:     false,
	serviceURL:  "https://service.cos.myqcloud.com",
	blockSize:   50,
	maxKeys:     1000,
}

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "coscli",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		globalConfig.secretID = viper.GetString("secret_id")
		globalConfig.secretKey = viper.GetString("secret_key")
		globalConfig.bucketName = viper.GetString("bucket_name")
		globalConfig.timeout = viper.GetInt("timeout")
		globalConfig.maxKeys = viper.GetInt("max_keys")
		globalConfig.blockSize = viper.GetInt("block_size")
		globalConfig.bucketURL = viper.GetString("base_url.bucket")
		globalConfig.bucketURLTpl = template.Must(
			template.New("bucketURLTpl").Parse(
				globalConfig.bucketURL,
			),
		)
		globalConfig.verbose = viper.GetBool("verbose")

		authTransport = &cos.AuthorizationTransport{
			SecretID:  globalConfig.secretID,
			SecretKey: globalConfig.secretKey,
		}

		formatter := new(prefixed.TextFormatter)
		formatter.FullTimestamp = true
		//formatter.SetColorScheme(&prefixed.ColorScheme{
		//	PrefixStyle:    "blue+b",
		//})
		log.Formatter = formatter
		if globalConfig.verbose {
			log.Level = logrus.DebugLevel
		}
	},
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.coscli.yaml)")
	RootCmd.PersistentFlags().StringVarP(&globalConfig.bucketName, "bucket", "b", "", "bucket Name")
	viper.BindPFlag("bucket_name", RootCmd.PersistentFlags().Lookup("bucket"))

	RootCmd.PersistentFlags().IntVar(&globalConfig.blockSize, "block-size", globalConfig.blockSize, "block size for download/upload (unit: MB)")
	viper.BindPFlag("block_size", RootCmd.PersistentFlags().Lookup("block-size"))

	RootCmd.PersistentFlags().StringVar(&globalConfig.secretID, "secret-id", "", "secret id")
	viper.BindPFlag("secret_id", RootCmd.PersistentFlags().Lookup("secret-id"))

	RootCmd.PersistentFlags().StringVar(&globalConfig.secretKey, "secret-key", "", "secret key")
	viper.BindPFlag("secret_key", RootCmd.PersistentFlags().Lookup("secret-key"))

	RootCmd.PersistentFlags().StringVar(&globalConfig.bucketURL, "bucket-url", "", "the base url of bucket")
	viper.BindPFlag("base_url.bucket", RootCmd.PersistentFlags().Lookup("bucket-url"))

	RootCmd.PersistentFlags().StringVar(&globalConfig.serviceURL, "service-url", globalConfig.serviceURL, "the base url of service")
	viper.BindPFlag("base_url.service", RootCmd.PersistentFlags().Lookup("service-url"))

	RootCmd.PersistentFlags().IntVarP(&globalConfig.concurrency, "concurrency", "c", globalConfig.concurrency, "max number of concurrency")
	viper.BindPFlag("concurrency", RootCmd.PersistentFlags().Lookup("concurrency"))

	RootCmd.PersistentFlags().IntVarP(&globalConfig.timeout, "timeout", "t", globalConfig.timeout, "timeout of request (unit: second)")
	viper.BindPFlag("timeout", RootCmd.PersistentFlags().Lookup("timeout"))

	RootCmd.PersistentFlags().BoolVarP(&globalConfig.verbose, "verbose", "v", globalConfig.verbose, "Verbose output")
	viper.BindPFlag("verbose", RootCmd.PersistentFlags().Lookup("verbose"))

	RootCmd.PersistentFlags().IntVar(&globalConfig.maxKeys, "max-keys", globalConfig.maxKeys, "max-keys")
	viper.BindPFlag("max_keys", RootCmd.PersistentFlags().Lookup("max-keys"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".coscli" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".coscli")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		//fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
