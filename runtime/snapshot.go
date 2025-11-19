package runtime

import (
	"fmt"
	"time"

	"github.com/tactors/sdk/actors"
	"github.com/tactors/sdk/internal/codec"
	"go.temporal.io/sdk/workflow"
)

const snapshotMemoKey = "__actors_snapshot"

type snapshotRecord struct {
	State   []byte
	Signals []snapshotSignal
	Stats   snapshotStats
}

type snapshotSignal struct {
	Name    string
	Payload []byte
}

type snapshotStats struct {
	SnapshotsTaken   int
	ContinueCount    int
	LastSnapshotTime time.Time
	SnapshotEvery    int
}

func (i *temporalInstance) snapshotAndContinue(ctx workflow.Context, wfCtx *wfContext, state any, chans map[string]workflow.ReceiveChannel, override any) (any, error) {
	record, err := i.buildSnapshot(ctx, chans, state)
	if err != nil {
		return nil, err
	}
	now := workflow.Now(ctx)
	stats := wfCtx.captureSnapshotStats(now, i.desc.SnapshotEvery)
	record.Stats = stats
	if err := workflow.UpsertMemo(ctx, map[string]any{snapshotMemoKey: record}); err != nil {
		return nil, err
	}
	var args any
	switch {
	case override != nil:
		args = override
	case i.desc.SnapshotArgs != nil:
		args, err = i.desc.SnapshotArgs(state)
		if err != nil {
			return nil, err
		}
	default:
		args = i.initialPayload
	}
	info := workflow.GetInfo(ctx)
	if info == nil || info.WorkflowType.Name == "" {
		return nil, fmt.Errorf("actors: workflow info unavailable")
	}
	i.processedSinceRotate = 0
	if args == nil {
		return nil, workflow.NewContinueAsNewError(ctx, info.WorkflowType.Name)
	}
	return args, workflow.NewContinueAsNewError(ctx, info.WorkflowType.Name, args)
}

func (i *temporalInstance) buildSnapshot(ctx workflow.Context, chans map[string]workflow.ReceiveChannel, state any) (snapshotRecord, error) {
	var record snapshotRecord
	if state != nil {
		blob, err := codec.Marshal(state)
		if err != nil {
			return record, err
		}
		record.State = blob
	}
	signals, err := i.drainSignals(ctx, chans)
	if err != nil {
		return record, err
	}
	record.Signals = signals
	return record, nil
}

func (i *temporalInstance) drainSignals(ctx workflow.Context, chans map[string]workflow.ReceiveChannel) ([]snapshotSignal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("actors: snapshot drain requires workflow context")
	}
	if len(chans) == 0 {
		return nil, nil
	}
	var drained []snapshotSignal
	var drainErr error
	selector := workflow.NewSelector(ctx)
	for name, ch := range chans {
		spec := i.desc.Commands[name]
		name := name
		ch := ch
		selector.AddReceive(ch, func(rc workflow.ReceiveChannel, more bool) {
			if drainErr != nil {
				return
			}
			payload, err := receiveCommandPayload(ctx, rc, spec)
			if err != nil {
				drainErr = err
				return
			}
			bytes, err := codec.Marshal(payload)
			if err != nil {
				drainErr = err
				return
			}
			drained = append(drained, snapshotSignal{Name: name, Payload: bytes})
		})
	}
	for pendingSignalCount(chans) > 0 {
		selector.Select(ctx)
		if drainErr != nil {
			return nil, drainErr
		}
	}
	return drained, nil
}

func pendingSignalCount(chans map[string]workflow.ReceiveChannel) int {
	total := 0
	for _, ch := range chans {
		total += ch.Len()
	}
	return total
}

func (i *temporalInstance) restoreSnapshot(ctx workflow.Context, state any) (snapshotRecord, error) {
	var record snapshotRecord
	info := workflow.GetInfo(ctx)
	if info == nil || info.Memo == nil || info.Memo.Fields == nil {
		return record, nil
	}
	payload, ok := info.Memo.Fields[snapshotMemoKey]
	if !ok {
		return record, nil
	}
	if err := dataConverter().FromPayload(payload, &record); err != nil {
		return snapshotRecord{}, err
	}
	if len(record.State) > 0 && state != nil {
		if err := codec.Unmarshal(record.State, state); err != nil {
			return snapshotRecord{}, err
		}
	}
	return record, nil
}

func (i *temporalInstance) replaySnapshotSignals(ctx workflow.Context, wfCtx *wfContext, state any, signals []snapshotSignal) error {
	logger := workflow.GetLogger(ctx)
	for _, sig := range signals {
		spec, ok := i.desc.Commands[sig.Name]
		if !ok {
			logger.Warn("snapshot signal discarded", "command", sig.Name)
			continue
		}
		var payload any
		if spec.PayloadFactory != nil {
			holder := spec.PayloadFactory()
			if err := codec.Unmarshal(sig.Payload, holder); err != nil {
				return err
			}
			if spec.DecodePayload != nil {
				decoded, err := spec.DecodePayload(holder)
				if err != nil {
					return err
				}
				payload = decoded
			} else {
				payload = holder
			}
		} else {
			var raw any
			if err := codec.Unmarshal(sig.Payload, &raw); err != nil {
				return err
			}
			payload = raw
		}
		if _, err := i.handleCommand(ctx, wfCtx, state, spec, payload, logger, sig.Name, actors.MessageMetadata{}); err != nil {
			return err
		}
		if i.desc.SnapshotEvery > 0 {
			i.processedSinceRotate++
		}
	}
	return nil
}
