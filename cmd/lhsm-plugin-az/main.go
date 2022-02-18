package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"
	"sync"
	"strconv"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"

	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/audit"
	"github.com/intel-hpdd/logging/debug"
	"github.com/wastore/go-lustre/llapi"
	"github.com/wastore/lemur/cmd/util"
	"github.com/wastore/lemur/dmplugin"
	"github.com/wastore/lemur/pkg/fsroot"
)

type (
	archiveConfig struct {
		Name                  string `hcl:",key"`
		ID                    int
		configLock            sync.Mutex //currently only SAS is protected by this lock
		AzStorageAccount      string `hcl:"az_storage_account"`
		HNSOverride           string `hcl:"hns_enabled"`
		HNSEnabled            bool 
		AzStorageKVName       string `hcl:"az_kv_name"`
		AzStorageKVSecretName string `hcl:"az_kv_secret_name"`
		AzStorageSAS          string
		Endpoint              string
		Region                string
		Container             string
		Prefix                string
		UploadPartSize        int64  `hcl:"upload_part_size"`
		NumThreads            int    `hcl:"num_threads"`
		Bandwidth             int    `hcl:"bandwidth"`
		MountRoot             string `hcl:"mountroot"`
		ExportPrefix          string `hcl:"exportprefix"`
		CredRefreshInterval   string `hcl:"cred_refresh_interval"`
		azCreds               azblob.Credential
	}

	archiveSet []*archiveConfig

	azConfig struct {
		NumThreads            int        `hcl:"num_threads"`
		AzStorageAccount      string     `hcl:"az_storage_account"`
		AzStorageKVName       string     `hcl:"az_kv_name"`
		AzStorageKVSecretName string     `hcl:"az_kv_secret_name"`
		Endpoint              string     `hcl:"endpoint"`
		Region                string     `hcl:"region"`
		UploadPartSize        int64      `hcl:"upload_part_size"`
		Archives              archiveSet `hcl:"archive"`
		Bandwidth             int        `hcl:"bandwidth"`
		MountRoot             string     `hcl:"mountroot"`
		ExportPrefix          string     `hcl:"exportprefix"`
		EventFIFOPath         string     `hcl:"event_fifo_path"`
	}
)

// Should this be configurable?
const updateInterval = 10 * time.Second

var rate metrics.Meter

func (c *azConfig) String() string {
	return dmplugin.DisplayConfig(c)
}

func (a *archiveConfig) String() string {
	return fmt.Sprintf("%d:%s:%s:%s/%s", a.ID, a.Endpoint, a.Region, a.Container, a.Prefix)
}

func (a *archiveConfig) checkValid() error {
	var errors []string

	if a.Container == "" {
		errors = append(errors, fmt.Sprintf("Archive %s: Container not set", a.Name))
	}

	if a.ID < 1 {
		errors = append(errors, fmt.Sprintf("Archive %s: archive id not set", a.Name))

	}

	/*
		if a.UploadPartSize < s3manager.MinUploadPartSize {
			errors = append(errors, fmt.Sprintf("Archive %s: upload_part_size %d is less than minimum (%d)", a.Name, a.UploadPartSize, s3manager.MinUploadPartSize))
		}
	*/
	if len(errors) > 0 {
		return fmt.Errorf("Errors: %s", strings.Join(errors, ", "))
	}

	return nil
}

func (a *archiveConfig) checkAzAccess() (err error) {
	const sigAzure string = "sig="
	if a.AzStorageKVName == "" || a.AzStorageKVSecretName == "" {
		return errors.New("No Az credentials found; cannot initialize data mover")
	}

	a.configLock.Lock()
	defer a.configLock.Unlock()

	a.AzStorageSAS, err = util.GetKVSecret(a.AzStorageKVName, a.AzStorageKVSecretName)

	if err != nil {
		return errors.Wrap(err, "Could not get secret. Check KV credentials.")
	}

	if !util.IsSASValid(a.AzStorageSAS) {
		return errors.New("Invalid SAS returned")
	}

	return nil
}

func (a *archiveConfig) setAccountType() (err error) {
	if val, err := strconv.ParseBool(a.HNSOverride); a.HNSOverride != "" && err == nil {
		a.HNSEnabled = val
		return nil
	}
	sURL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", a.AzStorageAccount, a.AzStorageSAS))
	serviceURL := azblob.NewServiceURL(*sURL, azblob.NewPipeline(a.azCreds, azblob.PipelineOptions{}))

	resp, err := serviceURL.GetAccountInfo(context.Background())
	if err != nil {
		return err
	}

	a.HNSEnabled = resp.Response().Header.Get("X-Ms-Is-Hns-Enabled") == "true"

	return nil
}

// this does an in-place merge, replacing any unset archive-level value
// with the global value for that setting
func (a *archiveConfig) mergeGlobals(g *azConfig) {
	if a.AzStorageAccount == "" {
		a.AzStorageAccount = g.AzStorageAccount
	}

	if a.AzStorageKVName == "" {
		a.AzStorageKVName = g.AzStorageKVName
	}

	if a.AzStorageKVSecretName == "" {
		a.AzStorageKVSecretName = g.AzStorageKVSecretName
	}

	if a.Endpoint == "" {
		a.Endpoint = g.Endpoint
	}

	if a.Region == "" {
		a.Region = g.Region
	}

	if a.UploadPartSize == 0 {
		a.UploadPartSize = g.UploadPartSize
	} else {
		// Allow this to be configured in MiB
		a.UploadPartSize *= 1024 * 1024
	}

	// If these were set on a per-archive basis, override the defaults.
	a.azCreds = azblob.NewAnonymousCredential()
	a.Bandwidth = g.Bandwidth
	a.MountRoot = g.MountRoot
	a.ExportPrefix = g.ExportPrefix

	if _, err := time.ParseDuration(a.CredRefreshInterval); err != nil {
		//Empty string or could not parse. We'll choose a default of 12hrs
		a.CredRefreshInterval = "12h"
	}
}

func (c *azConfig) Merge(other *azConfig) *azConfig {
	result := new(azConfig)

	result.UploadPartSize = c.UploadPartSize
	if other.UploadPartSize > 0 {
		result.UploadPartSize = other.UploadPartSize
	}

	result.NumThreads = c.NumThreads
	if other.NumThreads > 0 {
		result.NumThreads = other.NumThreads
	}

	result.Region = c.Region
	if other.Region != "" {
		result.Region = other.Region
	}

	result.Endpoint = c.Endpoint
	if other.Endpoint != "" {
		result.Endpoint = other.Endpoint
	}

	result.AzStorageAccount = c.AzStorageAccount
	if other.AzStorageAccount != "" {
		result.AzStorageAccount = other.AzStorageAccount
	}

	result.AzStorageKVName = c.AzStorageKVName
	if other.AzStorageKVName != "" {
		result.AzStorageKVName = other.AzStorageKVName
	}

	result.AzStorageKVSecretName = c.AzStorageKVSecretName
	if other.AzStorageKVSecretName != "" {
		result.AzStorageKVSecretName = other.AzStorageKVSecretName
	}

	result.Archives = c.Archives
	if len(other.Archives) > 0 {
		result.Archives = other.Archives
	}

	result.Bandwidth = c.Bandwidth
	if other.Bandwidth != 0 {
		result.Bandwidth = other.Bandwidth
	}

	result.MountRoot = c.MountRoot
	if other.MountRoot != "" {
		result.MountRoot = other.MountRoot
	}

	result.ExportPrefix = c.ExportPrefix
	if other.ExportPrefix != "" {
		result.ExportPrefix = other.ExportPrefix
	}

	result.EventFIFOPath = c.EventFIFOPath
	if other.EventFIFOPath != "" {
		result.EventFIFOPath = other.EventFIFOPath
	}

	return result
}

func init() {
	rate = metrics.NewMeter()

	// if debug.Enabled() {
	go func() {
		var lastCount int64
		for {
			if lastCount != rate.Count() {
				audit.Logf("total %s (1 min/5 min/15 min/inst): %s/%s/%s/%s msg/sec\n",
					humanize.Comma(rate.Count()),
					humanize.Comma(int64(rate.Rate1())),
					humanize.Comma(int64(rate.Rate5())),
					humanize.Comma(int64(rate.Rate15())),
					humanize.Comma(int64(rate.RateMean())),
				)
				lastCount = rate.Count()
			}
			time.Sleep(10 * time.Second)
		}
	}()
	// }
}

func getCredential(ac *archiveConfig) azblob.Credential {
	return ac.azCreds
}

func getMergedConfig(plugin *dmplugin.Plugin) (*azConfig, error) {
	baseCfg := &azConfig{
		Region:         "westeurope",
		UploadPartSize: 8388608,
	}

	var cfg azConfig
	err := dmplugin.LoadConfig(plugin.ConfigFile(), &cfg)

	if err != nil {
		return nil, fmt.Errorf("Failed to load config: %s", err)
	}

	// Allow this to be configured in MiB
	if cfg.UploadPartSize != 0 {
		cfg.UploadPartSize *= 1024 * 1024
	}

	return baseCfg.Merge(&cfg), nil
}

func main() {
	plugin, err := dmplugin.New(path.Base(os.Args[0]), func(path string) (fsroot.Client, error) {
		return fsroot.New(path)
	})
	if err != nil {
		alert.Abort(errors.Wrap(err, "failed to initialize plugin"))
	}
	defer plugin.Close()

	cfg, err := getMergedConfig(plugin)
	if err != nil {
		alert.Abort(errors.Wrap(err, "Unable to determine plugin configuration"))
	}

	if len(cfg.Archives) == 0 {
		alert.Abort(errors.New("Invalid configuration: No archives defined"))
	}

	rc, err := llapi.RegisterErrorCB(cfg.EventFIFOPath)
	if rc != 0 || err != nil {
		alert.Abort(errors.Wrap(err, "registering HSM event FIFO (plugin)"))
	}
	defer llapi.UnregisterErrorCB(cfg.EventFIFOPath)
	util.InitJobLogger(pipeline.LogDebug)

	for _, ac := range cfg.Archives {
		ac.mergeGlobals(cfg)
		if err = ac.checkValid(); err != nil {
			alert.Abort(errors.Wrap(err, "Invalid configuration"))
		}
		if err = ac.checkAzAccess(); err != nil {
			alert.Abort(errors.Wrap(err, "Az access check failed"))
		}
		if err = ac.setAccountType(); err != nil {
			alert.Abort(errors.Wrap(err, "Failed to set account type"))
		}
	}

	debug.Printf("AZMover configuration:\n%v", cfg)

	// All base filesystem operations will be relative to current directory
	err = os.Chdir(plugin.Base())
	if err != nil {
		alert.Abort(errors.Wrap(err, "chdir failed"))
	}

	interruptHandler(func() {
		plugin.Stop()
	})

	for _, ac := range cfg.Archives {
		plugin.AddMover(&dmplugin.Config{
			Mover:      AzMover(ac, getCredential(ac), uint32(ac.ID)),
			NumThreads: cfg.NumThreads,
			ArchiveID:  uint32(ac.ID),
		})
	}

	plugin.Run()
}

func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			debug.Printf("signal received: %s", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()
}
