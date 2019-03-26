// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"

	"github.com/edwardsp/lemur/dmplugin"
	"github.com/edwardsp/lemur/pkg/fsroot"
	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/audit"
	"github.com/intel-hpdd/logging/debug"
)

type (
	archiveConfig struct {
		Name             string `hcl:",key"`
		ID               int
		AzStorageAccount string `hcl:"az_storage_account"`
		AzStorageKey     string `hcl:"az_storage_key"`
		Endpoint         string
		Region           string
		Container        string
		Prefix           string
		UploadPartSize   int64 `hcl:"upload_part_size"`
		NumThreads       int   `hcl:"num_threads"`

		azCreds *azblob.SharedKeyCredential
	}

	archiveSet []*archiveConfig

	azConfig struct {
		NumThreads       int        `hcl:"num_threads"`
		AzStorageAccount string     `hcl:"az_storage_account"`
		AzStorageKey     string     `hcl:"az_storage_key"`
		Endpoint         string     `hcl:"endpoint"`
		Region           string     `hcl:"region"`
		UploadPartSize   int64      `hcl:"upload_part_size"`
		Archives         archiveSet `hcl:"archive"`
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

func (a *archiveConfig) checkAzAccess() error {
	if _, err := azblob.NewSharedKeyCredential(a.AzStorageAccount, a.AzStorageKey); err != nil {
		return errors.Wrap(err, "No Az credentials found; cannot initialize data mover")
	}

	/*
		if _, err := s3Svc(a).ListObjects(&s3.ListObjectsInput{
			Container: aws.String(a.Container),
		}); err != nil {
			return errors.Wrap(err, "Unable to list S3 Container objects")
		}
	*/
	return nil
}

// this does an in-place merge, replacing any unset archive-level value
// with the global value for that setting
func (a *archiveConfig) mergeGlobals(g *azConfig) {
	if a.AzStorageAccount == "" {
		a.AzStorageAccount = g.AzStorageAccount
	}

	if a.AzStorageKey == "" {
		a.AzStorageKey = g.AzStorageKey
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
	if a.AzStorageAccount != "" && a.AzStorageKey != "" {
		creds, _ := azblob.NewSharedKeyCredential(a.AzStorageAccount, a.AzStorageKey)
		a.azCreds = creds
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

	result.AzStorageKey = c.AzStorageKey
	if other.AzStorageKey != "" {
		result.AzStorageKey = other.AzStorageKey
	}

	result.Archives = c.Archives
	if len(other.Archives) > 0 {
		result.Archives = other.Archives
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

func s3Svc(ac *archiveConfig) *azblob.SharedKeyCredential {
	creds, _ := azblob.NewSharedKeyCredential(ac.AzStorageAccount, ac.AzStorageKey)
	return creds
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

	for _, ac := range cfg.Archives {
		ac.mergeGlobals(cfg)
		if err = ac.checkValid(); err != nil {
			alert.Abort(errors.Wrap(err, "Invalid configuration"))
		}
		if err = ac.checkAzAccess(); err != nil {
			alert.Abort(errors.Wrap(err, "Az access check failed"))
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
			Mover:      AzMover(ac, s3Svc(ac), uint32(ac.ID)),
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
