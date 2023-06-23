// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package dmplugin

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/debug"
	"github.com/wastore/lemur/cmd/util"
	pb "github.com/wastore/lemur/pdm"
	"golang.org/x/net/context"
)

type (
	// ActionHandler is function that implements one of the commands
	ActionHandler func(context.Context, Action) error

	actionFunc func()

	// DataMoverClient is the data mover client to the HSM agent
	DataMoverClient struct {
		plugin    *Plugin
		rpcClient pb.DataMoverClient
		status    chan *pb.ActionStatus
		mover     Mover
		config    *Config
		actions   map[pb.Command]ActionHandler
		cancelMap sync.Map
	}

	// Config defines configuration for a DatamMoverClient
	Config struct {
		Mover      Mover
		NumThreads int
		ArchiveID  uint32
		ActionQueueSize int
		ProgressUpdateMinutes int
	}

	// Action is a data movement action
	dmAction struct {
		ctx          context.Context
		status       chan *pb.ActionStatus
		item         *pb.ActionItem
		actualLength *int64
		uuid         string
		hash         []byte
		url          string
	}

	// Action defines an interface for dm actions
	Action interface {
		// Update sends an action status update
		Update(offset, length, max int64) error
		// ID returns the action item's ID
		ID() uint64
		// Offset returns the current offset of the action item
		Offset() int64
		// Length returns the expected length of the action item's file
		Length() int64
		// Data returns a byte slice of the action item's data
		Data() []byte
		// PrimaryPath returns the action item's primary file path
		PrimaryPath() string

		// WritePath returns the action item's write path (e.g. for restores)
		WritePath() string

		// UUID returns the action item's file id
		UUID() string

		// Hash returns the action item's file id
		Hash() []byte

		// URL returns the action item's file id
		URL() string

		// SetUUID sets the action's file id
		SetUUID(id string)

		// SetHash sets the action's file id
		SetHash(hash []byte)

		// SetURL sets the action's file id
		SetURL(id string)

		// SetActualLength sets the action's actual file length
		SetActualLength(length int64)
	}

	// Mover defines an interface for data mover implementations
	Mover interface {
		Start()
	}

	// Archiver defines an interface for data movers capable of
	// fulfilling Archive requests
	Archiver interface {
		Archive(context.Context, Action) error
	}

	// Restorer defines an interface for data movers capable of
	// fulfilling Restore requests
	Restorer interface {
		Restore(context.Context, Action) error
	}

	// Remover defines an interface for data movers capable of
	// fulfilling Remove requests
	Remover interface {
		Remove(context.Context, Action) error
	}
)

type key int

var handleKey key

const (
	defaultNumThreads = 4
)

const (
	defaultProgressUpdateMinutes = 3
)

func withHandle(ctx context.Context, handle *pb.Handle) context.Context {
	return context.WithValue(ctx, handleKey, handle)
}

func getHandle(ctx context.Context) (*pb.Handle, bool) {
	handle, ok := ctx.Value(handleKey).(*pb.Handle)
	return handle, ok
}

func (a *dmAction) String() string {
	return fmt.Sprintf("%v uuid:'%s' actualSize:%v", a.item, a.uuid, a.actualLength)
}

// Update sends an action status update
func (a *dmAction) Update(offset, length, max int64) error {
	a.status <- &pb.ActionStatus{
		Id:     a.item.Id,
		Offset: offset,
		Length: length,
	}
	return nil
}

// Finish finalizes the action.
func (a *dmAction) Finish(err error) {
	if err != nil {
		a.fail(err)
	} else {
		a.complete()
	}
}

// Complete signals that the action has completed
func (a *dmAction) complete() error {
	status := &pb.ActionStatus{
		Id:        a.item.Id,
		Completed: true,
		Offset:    a.item.Offset,
		Length:    a.item.Length,
		Uuid:      a.uuid,
		Hash:      a.hash,
		Url:       a.url,
	}
	if a.actualLength != nil {
		status.Length = *a.actualLength
	}
	a.status <- status
	return nil
}

func getErrno(err error) int32 {
	if errno, ok := err.(syscall.Errno); ok {
		return int32(errno)
	}
	return -1
}

// Fail signals that the action has failed
func (a *dmAction) fail(err error) error {
	sanitizedErr := errors.New(util.NewAzCopyLogSanitizer().SanitizeLogMessage(err.Error()))
	alert.Warnf("fail: id:%d %v", a.item.Id, sanitizedErr)
	a.status <- &pb.ActionStatus{
		Id:        a.item.Id,
		Completed: true,

		Error: util.UnixError(err),
	}
	return nil
}

// ID returns the action item's ID
func (a *dmAction) ID() uint64 {
	return a.item.Id
}

// Offset returns the current offset of the action item
func (a *dmAction) Offset() int64 {
	return a.item.Offset
}

// Length returns the expected length of the action item's file
func (a *dmAction) Length() int64 {
	return a.item.Length
}

// Data returns a byte slice of the action item's data
func (a *dmAction) Data() []byte {
	return a.item.Data
}

// PrimaryPath returns the action item's primary file path
func (a *dmAction) PrimaryPath() string {
	return a.item.PrimaryPath
}

// WritePath returns the action item's write path (e.g. for restores)
func (a *dmAction) WritePath() string {
	return a.item.WritePath
}

// UUID returns the action item's file id
func (a *dmAction) UUID() string {
	return a.item.Uuid
}

// Hash returns the action item's file id
func (a *dmAction) Hash() []byte {
	return a.item.Hash
}

// URL returns the action item's file id
func (a *dmAction) URL() string {
	return a.item.Url
}

// SetUUID sets the action's file uuid
func (a *dmAction) SetUUID(id string) {
	a.uuid = id
}

// SetHash sets the action's file hash
func (a *dmAction) SetHash(h []byte) {
	a.hash = h
}

// SetURL sets the action's file id
func (a *dmAction) SetURL(u string) {
	a.url = u
}

// SetActualLength sets the action's actual file length
func (a *dmAction) SetActualLength(length int64) {
	a.actualLength = &length
}

// NewMover returns a new *DataMoverClient
func NewMover(plugin *Plugin, cli pb.DataMoverClient, config *Config) *DataMoverClient {
	actions := make(map[pb.Command]ActionHandler)

	if archiver, ok := config.Mover.(Archiver); ok {
		actions[pb.Command_ARCHIVE] = archiver.Archive
	}
	if restorer, ok := config.Mover.(Restorer); ok {
		actions[pb.Command_RESTORE] = restorer.Restore
	}
	if remover, ok := config.Mover.(Remover); ok {
		actions[pb.Command_REMOVE] = remover.Remove
	}

	return &DataMoverClient{
		plugin:    plugin,
		rpcClient: cli,
		mover:     config.Mover,
		status:    make(chan *pb.ActionStatus, config.NumThreads),
		config:    config,
		actions:   actions,
	}
}

// Run begins listening for and processing incoming action items
func (dm *DataMoverClient) Run(ctx context.Context) {
	var wg sync.WaitGroup

	handle, err := dm.registerEndpoint(ctx)
	if err != nil {
		alert.Abort(errors.Wrap(err, "register endpoint failed"))
	}
	ctx = withHandle(ctx, handle)
	actions := dm.processActions(ctx)
	dm.processStatus(ctx)

	n := defaultNumThreads
	if dm.config.NumThreads > 0 {
		n = dm.config.NumThreads
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			dm.handler(fmt.Sprintf("handler-%d", i), actions)
			wg.Done()
		}(i)
	}

	// Signal to the mover that it should begin any async processing
	dm.config.Mover.Start()

	wg.Wait()
	debug.Printf("Shutting down Data Mover")
	close(dm.status)
}

func (dm *DataMoverClient) defaultThreadCount() int {
	if dm.config.NumThreads > 0 {
		return dm.config.NumThreads
	}

	return defaultNumThreads
}

func (dm *DataMoverClient) defaultQueueSize() int {
	if dm.config.ActionQueueSize > 0 {
		return dm.config.ActionQueueSize
	}

	return dm.defaultThreadCount() * 2
}

func (dm *DataMoverClient) defaultUpdateMinutes() int {
	if dm.config.ProgressUpdateMinutes > 0 {
		return dm.config.ProgressUpdateMinutes
	}

	return defaultProgressUpdateMinutes
}

func (dm *DataMoverClient) registerEndpoint(ctx context.Context) (*pb.Handle, error) {

	handle, err := dm.rpcClient.Register(ctx, &pb.Endpoint{
		FsUrl:   dm.plugin.FsName(),
		Archive: dm.config.ArchiveID,
	})
	if err != nil {
		return nil, err
	}
	debug.Printf("Registered archive %d,  cookie %x", dm.config.ArchiveID, handle.Id)
	return handle, nil
}

func (dm *DataMoverClient) processActions(ctx context.Context) chan *dmAction {
	actions := make(chan *dmAction, dm.defaultQueueSize())
	cancelAction := func(action *dmAction) {
		// lookup in the map and cancel the context
		cancel, ok := dm.cancelMap.Load(action.PrimaryPath())
		if !ok {
			msg := fmt.Sprintf("Received cancel for a non-existent action: %s", action.item.PrimaryPath)
			alert.Warnf(msg)
			// If we receive a cancel action for something non-existent, we must let the CDT know.  Otherwise a CANCEL action
			// will remain in the actions queue indefinitely.
			action.Finish(errors.New(msg))
			return
		}

		alert.Writer().Log(fmt.Sprintf("id:%d Cancel %s", action.item.Id, action.item.PrimaryPath))
		cancel.(context.CancelFunc)()
		// Do not call action.Finish.  The action being canceled will call Finish with an error indicating cancelation.
		// In the case of hsm_cancel, there will exist a CANCEL action in the action queue with an identical cookie to the
		// action being canceled.  The response by the original request being canceled will result in the termination of
		// both since they share a cookie.  In the case of cancel_archives (really a purge of ARCHIVE requests), there is no
		// twin sibling action in the actions queue, but the net-effect is the same.
	}

	go func() {
		defer close(actions)
		handle, ok := getHandle(ctx)
		if !ok {
			alert.Warn(errors.New("No context"))
			return
		}
		stream, err := dm.rpcClient.GetActions(ctx, handle)
		if err != nil {
			alert.Warn(errors.Wrap(err, "GetActions() failed"))
			return
		}
		for {
			item, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					debug.Print("Shutting down dmclient action stream")
					return
				}
				alert.Warnf("Shutting down dmclient action stream due to error on Recv(): %v", err)
				return
			}
			// debug.Printf("Got message id:%d op: %v %v", action.Id, action.Op, action.PrimaryPath)

			item.TryCount = 0 //first try
			c, cancel := context.WithCancel(ctx)
			action := &dmAction{ status: dm.status, item: item, ctx: c, }

			if (item.Op == pb.Command_CANCEL) {
				cancelAction(action)
			} else {
				/* if actions is full, we'll get to default clause */
				select {
				case actions <- action:
					/* Safe to be done after sending because these values are handled only by this thread */
					dm.cancelMap.Store(action.PrimaryPath(), cancel)
				default:
					alert.Warnf("Request limit reached. Failing action %s, command %d", action.PrimaryPath(), action.item.Op)
					action.Finish(util.CopytoolBusy)
				}
			}
		}

	}()

	return actions

}

func (dm *DataMoverClient) processStatus(ctx context.Context) {
	go func() {
		handle, ok := getHandle(ctx)
		if !ok {
			alert.Abort(errors.New("No context"))
		}
		acks, err := dm.rpcClient.StatusStream(ctx)
		if err != nil {
			alert.Warn(errors.Wrap(err, "StatusStream() failed"))
			return
		}
		for reply := range dm.status {
			reply.Handle = handle
			// debug.Printf("Sent reply  %x error: %#v", reply.Id, reply.Error)
			err := acks.Send(reply)
			if err != nil {
				alert.Warn(errors.Wrapf(err, "Failed to ack message %x", reply.Id))
				return
			}
		}
	}()
	return
}

// getActionHandler returns the mover's action function for the comamnd, or err
// if there is no handler for that command.
func (dm *DataMoverClient) getActionHandler(op pb.Command) (ActionHandler, error) {
	fn, ok := dm.actions[op]
	if !ok {
		return nil, errors.New("Command not supported")
	}
	return fn, nil
}

func (dm *DataMoverClient) handler(name string, actions chan *dmAction) {
	maxTryCount := 3
	if r := os.Getenv("COPYTOOL_RETRY_COUNT"); r != "" {
		if v, err := strconv.Atoi(r); err == nil {
			maxTryCount = v
		}
	}

	heartbeat := func(ctx context.Context, action *dmAction) {
		ticker := time.NewTicker(time.Duration(dm.defaultUpdateMinutes()) * time.Minute)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				action.Update(0, 0, 0)
			}
		}
	}

	for action := range actions {
		actionFn, err := dm.getActionHandler(action.item.Op)
		if err == nil {
			go heartbeat(action.ctx, action)
			err = actionFn(action.ctx, action)
		}
		// debug.Printf("completed (action: %v) %v ", action, ret)
		if util.ShouldRetry(err) && action.item.TryCount < int64(maxTryCount) {
			action.item.TryCount += 1
			debug.Printf("Retrying action %d.Trycount: %d. Error: %s", action.item.Id, action.item.TryCount, err.Error())
			go func() { actions <- action }()
		} else {
			// Delete from map before we finish
			cancel, ok := dm.cancelMap.LoadAndDelete(action.PrimaryPath())
			if ok {
				cancel.(context.CancelFunc)()
			}
			action.Finish(err)
		}

		if err != nil {
			err = errors.New(util.NewAzCopyLogSanitizer().SanitizeLogMessage(err.Error()))
		}
	}
	debug.Printf("%s: stopping", name)
}

//This needs to move to appropriate location, but we'll tolerate here
//for now

func shouldRetry(err error) bool {
	if stgErr, ok := err.(azblob.StorageError); ok {
		if stgErr.Response().StatusCode == 403 {
			return true
		}
	}

	return false
}
