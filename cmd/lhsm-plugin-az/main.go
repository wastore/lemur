package main

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/signal"
	"path"
	rdbg "runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"

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
		AzStorageAccountURL   string `hcl:"az_storage_account_url"`
		AzStorageKVURL        string `hcl:"az_kv_url"`
		AzStorageKVSecretName string `hcl:"az_kv_secret_name"`
		AzStorageSAS          string `json:"-"`
		SASContext            time.Time  `json:"-"` //Context is used by operations to know if they've latest SAS
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
		jobMgr                ste.IJobMgr `json:"-"`
		containerURL          *url.URL /* Container Blob Endpoint URL */
	}

	archiveSet []*archiveConfig

	azConfig struct {
		NumThreads            int        `hcl:"num_threads"`
		ActionQueueSize       int        `hcl:"action_queue_size"`
		AzStorageAccountURL   string     `hcl:"az_storage_account_url"`
		AzStorageKVURL        string     `hcl:"az_kv_url"`
		AzStorageKVSecretName string     `hcl:"az_kv_secret_name"`
		Endpoint              string     `hcl:"endpoint"`
		Region                string     `hcl:"region"`
		UploadPartSize        int64      `hcl:"upload_part_size"`
		Archives              archiveSet `hcl:"archive"`
		Bandwidth             int        `hcl:"bandwidth"`
		MountRoot             string     `hcl:"mountroot"`
		ExportPrefix          string     `hcl:"exportprefix"`
		EventFIFOPath         string     `hcl:"event_fifo_path"`

		/* STE Parameters */
		PlanDirectory string `hcl:"plan_dir"`
		CacheLimit    int    `hcl:"cache_limit"`
		LogLevel      string `hcl:"log_level"`
		jobMgr        ste.IJobMgr
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

func (a *archiveConfig) ContainerURL() *url.URL {
	return a.containerURL
}


func (a *archiveConfig) checkValid() error {
	var errors []string

	if _, err := url.Parse(a.AzStorageAccountURL); err != nil {
		errors = append(errors, "Archive %s: Invalid URL set for StorageAccount.", a.AzStorageAccountURL)
	}
	if a.Container == "" {
		errors = append(errors, fmt.Sprintf("Archive %s: Container not set", a.Name))
	}

	if a.AzStorageKVURL == "" || a.AzStorageKVSecretName == "" {
		errors = append(errors, fmt.Sprintf("Archive %s: Keyvault URL and secret name should not be empty."))
	}
	if _, err := url.Parse(a.AzStorageKVURL); err != nil {
		errors = append(errors, fmt.Sprintf("Archive %s: Invalid URL specified for Keyvault"))
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

	a.AzStorageSAS, err = util.GetKVSecret(a.AzStorageKVURL, a.AzStorageKVSecretName)

	if err != nil {
		return errors.Wrap(err, "Could not get secret. Check KV credentials.")
	}

	if ok, reason := util.IsSASValid(a.AzStorageSAS); !ok {
		return errors.New("Invalid SAS returned " + reason)
	}

	return nil
}

func (a *archiveConfig) setContainerURL() (error) {
	u, err := url.Parse(a.AzStorageAccountURL)
	if (err != nil) {
		return err
	}
	u.Path = path.Join(u.Path, a.Container)
	u.RawQuery = a.AzStorageSAS
	a.containerURL = u

	return nil
}

// this does an in-place merge, replacing any unset archive-level value
// with the global value for that setting
func (a *archiveConfig) mergeGlobals(g *azConfig) {
	if a.AzStorageAccountURL == "" {
		a.AzStorageAccountURL = g.AzStorageAccountURL
	}

	if a.AzStorageKVURL == "" {
		a.AzStorageKVURL = g.AzStorageKVURL
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
	const defaultSTEMemoryLimit = 2 /* In GiBs */
	defaultSTETempDir := path.Join(os.TempDir(), "copytool")

	result.UploadPartSize = c.UploadPartSize
	if other.UploadPartSize > 0 {
		result.UploadPartSize = other.UploadPartSize
	}

	result.NumThreads = c.NumThreads
	if other.NumThreads > 0 {
		result.NumThreads = other.NumThreads
	}

	result.ActionQueueSize = c.ActionQueueSize
	if other.ActionQueueSize > 0 {
		result.ActionQueueSize = other.ActionQueueSize
	}

	result.Region = c.Region
	if other.Region != "" {
		result.Region = other.Region
	}

	result.Endpoint = c.Endpoint
	if other.Endpoint != "" {
		result.Endpoint = other.Endpoint
	}

	result.AzStorageAccountURL = c.AzStorageAccountURL
	if other.AzStorageAccountURL != "" {
		result.AzStorageAccountURL = other.AzStorageAccountURL
	}

	result.AzStorageKVURL = c.AzStorageKVURL
	if other.AzStorageKVURL != "" {
		result.AzStorageKVURL = other.AzStorageKVURL
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

	/* STE parameters */
	result.CacheLimit = defaultSTEMemoryLimit
	if other.CacheLimit != 0 {
		result.CacheLimit = other.CacheLimit
	}
	
	result.LogLevel = c.LogLevel
	if other.LogLevel != "" {
		result.LogLevel = other.LogLevel
	}
	
	result.PlanDirectory = defaultSTETempDir
	if other.PlanDirectory != "" {
		result.PlanDirectory = other.PlanDirectory
	}
	
	return result
}

func (a *azConfig) initSTE() (err error) {
	jobID := common.NewJobID()
	tuner := ste.NullConcurrencyTuner{FixedValue: 128}
	var pacer ste.PacerAdmin = ste.NewNullAutoPacer()
	var logLevel common.LogLevel
	common.AzcopyJobPlanFolder = a.PlanDirectory

	if err := logLevel.Parse(a.LogLevel); err != nil {
		logLevel = common.ELogLevel.Info()
	}
	logger := common.NewSysLogger(jobID, logLevel, "lhsm-plugin-az")
	logger.OpenLog()

	os.MkdirAll(common.AzcopyJobPlanFolder, 0666)
	if a.Bandwidth != 0 {
		pacer = ste.NewTokenBucketPacer(int64(a.Bandwidth*1024*1024), 0)
	}

	a.jobMgr = ste.NewJobMgr(ste.NewConcurrencySettings(math.MaxInt32, false),
		jobID,
		context.Background(),
		common.NewNullCpuMonitor(),
		common.ELogLevel.Error(),
		"Lustre",
		a.PlanDirectory,
		&tuner,
		pacer,
		common.NewMultiSizeSlicePool(4*1024*1024*1024 /* 4GiG */),
		common.NewCacheLimiter(int64(a.CacheLimit*1024*1024*1024)),
		common.NewCacheLimiter(int64(64)),
		logger,
		true)

	/*
	 This needs to be moved to a better location
	*/
	go func() {
		time.Sleep(20 * time.Second) // wait a little, so that our initial pool of buffers can get allocated without heaps of (unnecessary) GC activity
		rdbg.SetGCPercent(20)        // activate more aggressive/frequent GC than the default
	}()

	util.SetJobMgr(a.jobMgr)
	util.ResetPartNum()
	common.GetLifecycleMgr().E2EEnableAwaitAllowOpenFiles(false)
	common.GetLifecycleMgr().SetForceLogging()

	return nil
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
	var minimumLevelToLog pipeline.LogLevel

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

	if err = cfg.initSTE(); err != nil {
		alert.Abort(errors.Wrap(err, "Failed to initialize STE"))
	}

	rc, err := llapi.RegisterErrorCB(cfg.EventFIFOPath)
	if rc != 0 || err != nil {
		alert.Abort(errors.Wrap(err, "registering HSM event FIFO (plugin)"))
	}
	defer llapi.UnregisterErrorCB(cfg.EventFIFOPath)

	switch cfg.LogLevel {
		case "none":
			minimumLevelToLog = pipeline.LogNone
		case "fatal":
			minimumLevelToLog = pipeline.LogFatal
		case "panic":
			minimumLevelToLog = pipeline.LogPanic
		case "error":
			minimumLevelToLog = pipeline.LogError
		case "warning":
			minimumLevelToLog = pipeline.LogWarning
		case "info":
			minimumLevelToLog = pipeline.LogInfo
		default:
			minimumLevelToLog = pipeline.LogDebug
	}
	util.InitJobLogger(minimumLevelToLog)

	for _, ac := range cfg.Archives {
		ac.mergeGlobals(cfg)
		if err = ac.checkValid(); err != nil {
			alert.Abort(errors.Wrap(err, "Invalid configuration"))
		}
		if err = ac.checkAzAccess(); err != nil {
			alert.Abort(errors.Wrap(err, "Az access check failed"))
		}
		if err = ac.setContainerURL(); err != nil {
			alert.Abort(errors.Wrap(err, "Failed to parse Account URL"))
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
			ActionQueueSize: cfg.ActionQueueSize,
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
