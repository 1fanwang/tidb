// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	driver "github.com/pingcap/tidb/pkg/store/driver/txn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// BatchPointGetExec executes a bunch of point select queries.
type BatchPointGetExec struct {
	exec.BaseExecutor
	indexUsageReporter *exec.IndexUsageReporter

	tblInfo *model.TableInfo
	idxInfo *model.IndexInfo
	handles []kv.Handle
	// table/partition IDs for handle or index read
	// (can be secondary unique key,
	// and need lookup through handle)
	planPhysIDs []int64
	// If != 0 then it is a single partition under Static Prune mode.
	singlePartID   int64
	partitionNames []ast.CIStr
	idxVals        [][]types.Datum
	txn  kv.Transaction
	lock bool
	// skipLocked indicates the lock clause is FOR UPDATE SKIP LOCKED: rows whose row
	// key or index key is locked by another transaction are filtered out instead of
	// waited for.
	skipLocked bool
	waitTime   int64
	inited         uint32
	values         [][]byte
	index          int
	rowDecoder     *rowcodec.ChunkDecoder
	keepOrder      bool
	desc           bool
	batchGetter    kv.BatchGetter

	columns []*model.ColumnInfo
	// virtualColumnIndex records all the indices of virtual columns and sort them in definition
	// to make sure we can compute the virtual column in right order.
	virtualColumnIndex []int

	// virtualColumnRetFieldTypes records the RetFieldTypes of virtual columns.
	virtualColumnRetFieldTypes []*types.FieldType

	snapshot kv.Snapshot
	stats    *runtimeStatsWithSnapshot
}

// buildVirtualColumnInfo saves virtual column indices and sort them in definition order
func (e *BatchPointGetExec) buildVirtualColumnInfo() {
	e.virtualColumnIndex = buildVirtualColumnIndex(e.Schema(), e.columns)
	if len(e.virtualColumnIndex) > 0 {
		e.virtualColumnRetFieldTypes = make([]*types.FieldType, len(e.virtualColumnIndex))
		for i, idx := range e.virtualColumnIndex {
			e.virtualColumnRetFieldTypes[i] = e.Schema().Columns[idx].RetType
		}
	}
}

// Open implements the Executor interface.
func (e *BatchPointGetExec) Open(context.Context) error {
	sessVars := e.Ctx().GetSessionVars()
	txnCtx := sessVars.TxnCtx
	txn, err := e.Ctx().Txn(false)
	if err != nil {
		return err
	}
	e.txn = txn

	setOptionForTopSQL(e.Ctx().GetSessionVars().StmtCtx, e.snapshot)
	var batchGetter kv.BatchGetter = e.snapshot
	if txn.Valid() {
		lock := e.tblInfo.Lock
		if e.lock {
			batchGetter = driver.NewBufferBatchGetter(txn.GetMemBuffer(), &PessimisticLockCacheGetter{txnCtx: txnCtx}, e.snapshot)
		} else if lock != nil && (lock.Tp == ast.TableLockRead || lock.Tp == ast.TableLockReadOnly) && e.Ctx().GetSessionVars().EnablePointGetCache {
			batchGetter = newCacheBatchGetter(e.Ctx(), e.tblInfo.ID, e.snapshot)
		} else {
			batchGetter = driver.NewBufferBatchGetter(txn.GetMemBuffer(), nil, e.snapshot)
		}
	}
	e.batchGetter = batchGetter
	return nil
}

// CacheTable always use memBuffer in session as snapshot.
// cacheTableSnapshot inherits kv.Snapshot and override the BatchGet methods and Get methods.
type cacheTableSnapshot struct {
	kv.Snapshot
	memBuffer kv.MemBuffer
}

func (s cacheTableSnapshot) BatchGet(ctx context.Context, keys []kv.Key, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	if len(options) > 0 {
		var opt tikv.BatchGetOptions
		opt.Apply(options)
		if opt.ReturnCommitTS() {
			return nil, errors.New("WithReturnCommitTS option is not supported for cacheTableSnapshot.BatchGet")
		}
	}
	values := make(map[string]kv.ValueEntry)
	if s.memBuffer == nil {
		return values, nil
	}

	getOptions := kv.BatchGetToGetOptions(options)
	for _, key := range keys {
		val, err := s.memBuffer.Get(ctx, key, getOptions...)
		if kv.ErrNotExist.Equal(err) {
			continue
		}

		if err != nil {
			return nil, err
		}

		if val.IsValueEmpty() {
			continue
		}

		values[string(key)] = val
	}

	return values, nil
}

func (s cacheTableSnapshot) Get(ctx context.Context, key kv.Key, options ...kv.GetOption) (kv.ValueEntry, error) {
	if len(options) > 0 {
		var opt tikv.GetOptions
		opt.Apply(options)
		if opt.ReturnCommitTS() {
			return kv.ValueEntry{}, errors.New("WithReturnCommitTS option is not supported for cacheTableSnapshot.Get")
		}
	}
	return s.memBuffer.Get(ctx, key, options...)
}

// MockNewCacheTableSnapShot only serves for test.
func MockNewCacheTableSnapShot(snapshot kv.Snapshot, memBuffer kv.MemBuffer) *cacheTableSnapshot {
	return &cacheTableSnapshot{snapshot, memBuffer}
}

// Close implements the Executor interface.
func (e *BatchPointGetExec) Close() error {
	if e.RuntimeStats() != nil {
		defer func() {
			sc := e.Ctx().GetSessionVars().StmtCtx
			sc.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
			timeDetail := e.stats.SnapshotRuntimeStats.GetTimeDetail()
			if timeDetail != nil {
				e.Ctx().GetSessionVars().SQLCPUUsages.MergeTikvCPUTime(timeDetail.ProcessTime)
			}
		}()
	}

	if e.RuntimeStats() != nil && e.snapshot != nil {
		e.snapshot.SetOption(kv.CollectRuntimeStats, nil)
	}
	if e.indexUsageReporter != nil && e.stats != nil {
		kvReqTotal := e.stats.GetCmdRPCCount(tikvrpc.CmdBatchGet)
		// We cannot distinguish how many rows are coming from each partition. Here, we calculate all index usages
		// percentage according to the row counts for the whole table.
		rows := e.RuntimeStats().GetActRows()
		if e.idxInfo != nil {
			e.indexUsageReporter.ReportPointGetIndexUsage(e.tblInfo.ID, e.tblInfo.ID, e.idxInfo.ID, kvReqTotal, rows)
		} else {
			e.indexUsageReporter.ReportPointGetIndexUsageForHandle(e.tblInfo, e.tblInfo.ID, kvReqTotal, rows)
		}
	}
	e.inited = 0
	e.index = 0
	return nil
}

// Next implements the Executor interface.
func (e *BatchPointGetExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if atomic.CompareAndSwapUint32(&e.inited, 0, 1) {
		if err := e.initialize(ctx); err != nil {
			return err
		}
		if e.lock {
			e.UpdateDeltaForTableID(e.tblInfo.ID)
		}
	}

	if e.index >= len(e.values) {
		return nil
	}

	schema := e.Schema()
	sctx := e.BaseExecutor.Ctx()
	start := e.index
	for !req.IsFull() && e.index < len(e.values) {
		handle, val := e.handles[e.index], e.values[e.index]
		err := DecodeRowValToChunk(sctx, schema, e.tblInfo, handle, val, req, e.rowDecoder)
		if err != nil {
			return err
		}
		e.index++
	}

	err := fillRowChecksum(sctx, start, e.index, schema, e.tblInfo, e.values, e.handles, req, nil)
	if err != nil {
		return err
	}
	err = table.FillVirtualColumnValue(e.virtualColumnRetFieldTypes, e.virtualColumnIndex, schema.Columns, e.columns, sctx.GetExprCtx(), req)
	if err != nil {
		return err
	}
	return nil
}

func (e *BatchPointGetExec) initialize(ctx context.Context) error {
	var handleVals map[string]kv.ValueEntry
	var indexKeys []kv.Key
	var err error
	batchGetter := e.batchGetter
	maxExecutionTime := e.Ctx().GetSessionVars().GetMaxExecutionTime()
	if maxExecutionTime > 0 {
		// If MaxExecutionTime is set, we need to set the context deadline for the batch get.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(maxExecutionTime)*time.Millisecond)
		defer cancel()
	}
	rc := e.Ctx().GetSessionVars().IsPessimisticReadConsistency()
	if e.idxInfo != nil && !isCommonHandleRead(e.tblInfo, e.idxInfo) {
		// `SELECT a, b FROM t WHERE (a, b) IN ((1, 2), (1, 2), (2, 1), (1, 2))` should not return duplicated rows
		dedup := make(map[hack.MutableString]struct{})
		toFetchIndexKeys := make([]kv.Key, 0, len(e.idxVals))
		for i, idxVals := range e.idxVals {
			physID := e.tblInfo.ID
			if e.singlePartID != 0 {
				physID = e.singlePartID
			} else if len(e.planPhysIDs) > i {
				physID = e.planPhysIDs[i]
			}
			idxKey, err1 := physicalop.EncodeUniqueIndexKey(e.Ctx(), e.tblInfo, e.idxInfo, idxVals, physID)
			if err1 != nil && !kv.ErrNotExist.Equal(err1) {
				return err1
			}
			if idxKey == nil {
				continue
			}
			s := hack.String(idxKey)
			if _, found := dedup[s]; found {
				continue
			}
			dedup[s] = struct{}{}
			toFetchIndexKeys = append(toFetchIndexKeys, idxKey)
		}
		if e.keepOrder {
			// TODO: if multiple partitions, then the IDs needs to be
			// in the same order as the index keys
			// and should skip table id part when comparing
			intest.Assert(e.singlePartID != 0 || len(e.planPhysIDs) <= 1 || e.idxInfo.Global)
			slices.SortFunc(toFetchIndexKeys, func(i, j kv.Key) int {
				if e.desc {
					return j.Cmp(i)
				}
				return i.Cmp(j)
			})
		}

		// lock all keys in repeatable read isolation.
		// for read consistency, only lock exist keys,
		// indexKeys will be generated after getting handles.
		if !rc {
			indexKeys = toFetchIndexKeys
		} else {
			indexKeys = make([]kv.Key, 0, len(toFetchIndexKeys))
		}

		// SELECT * FROM t WHERE x IN (null), in this case there is no key.
		if len(toFetchIndexKeys) == 0 {
			return nil
		}

		// Fetch all handles.
		handleVals, err = batchGetter.BatchGet(ctx, toFetchIndexKeys)
		if err != nil {
			return err
		}

		e.handles = make([]kv.Handle, 0, len(toFetchIndexKeys))
		if e.tblInfo.Partition != nil {
			e.planPhysIDs = e.planPhysIDs[:0]
		}
		for _, key := range toFetchIndexKeys {
			handleVal := handleVals[string(key)]
			if handleVal.IsValueEmpty() {
				continue
			}
			handle, err1 := tablecodec.DecodeHandleInIndexValue(handleVal.Value)
			if err1 != nil {
				return err1
			}
			if e.tblInfo.Partition != nil {
				var pid int64
				if e.idxInfo.Global {
					_, pid, err = codec.DecodeInt(tablecodec.SplitIndexValue(handleVal.Value).PartitionID)
					if err != nil {
						return err
					}
					if e.singlePartID != 0 && e.singlePartID != pid {
						continue
					}
					if !matchPartitionNames(pid, e.partitionNames, e.tblInfo.GetPartitionInfo()) {
						continue
					}
					e.planPhysIDs = append(e.planPhysIDs, pid)
				} else {
					pid = tablecodec.DecodeTableID(key)
					e.planPhysIDs = append(e.planPhysIDs, pid)
				}
				if e.lock {
					e.UpdateDeltaForTableID(pid)
				}
			}
			e.handles = append(e.handles, handle)
			if rc {
				indexKeys = append(indexKeys, key)
			}
		}

		// The injection is used to simulate following scenario:
		// 1. Session A create a point get query but pause before second time `GET` kv from backend
		// 2. Session B create an UPDATE query to update the record that will be obtained in step 1
		// 3. Then point get retrieve data from backend after step 2 finished
		// 4. Check the result
		failpoint.InjectContext(ctx, "batchPointGetRepeatableReadTest-step1", func() {
			if ch, ok := ctx.Value("batchPointGetRepeatableReadTest").(chan struct{}); ok {
				// Make `UPDATE` continue
				close(ch)
			}
			// Wait `UPDATE` finished
			failpoint.InjectContext(ctx, "batchPointGetRepeatableReadTest-step2", nil)
		})
	} else if e.keepOrder {
		less := func(i, j kv.Handle) int {
			if e.desc {
				return j.Compare(i)
			}
			return i.Compare(j)
		}
		if e.tblInfo.PKIsHandle && mysql.HasUnsignedFlag(e.tblInfo.GetPkColInfo().GetFlag()) {
			uintComparator := func(i, h kv.Handle) int {
				if !i.IsInt() || !h.IsInt() {
					panic(fmt.Sprintf("both handles need be IntHandle, but got %T and %T ", i, h))
				}
				ihVal := uint64(i.IntValue())
				hVal := uint64(h.IntValue())
				if ihVal > hVal {
					return 1
				}
				if ihVal < hVal {
					return -1
				}
				return 0
			}
			less = func(i, j kv.Handle) int {
				if e.desc {
					return uintComparator(j, i)
				}
				return uintComparator(i, j)
			}
		}
		slices.SortFunc(e.handles, less)
		// TODO: if partitioned table, sorting the handles would also
		//  need to have the physIDs rearranged in the same order!
		intest.Assert(e.singlePartID != 0 || len(e.planPhysIDs) <= 1)
	}

	keys := make([]kv.Key, 0, len(e.handles))
	newHandles := make([]kv.Handle, 0, len(e.handles))
	for i, handle := range e.handles {
		tID := e.tblInfo.ID
		if e.singlePartID != 0 {
			tID = e.singlePartID
		} else if len(e.planPhysIDs) > 0 {
			// Direct handle read
			tID = e.planPhysIDs[i]
		}
		if tID <= 0 {
			// not matching any partition
			continue
		}
		key := tablecodec.EncodeRowKeyWithHandle(tID, handle)
		keys = append(keys, key)
		newHandles = append(newHandles, handle)
	}
	e.handles = newHandles

	var values map[string]kv.ValueEntry
	// Lock keys (include exists and non-exists keys) before fetch all values for Repeatable Read Isolation.
	if e.lock && !rc {
		lockKeys := make([]kv.Key, len(keys)+len(indexKeys))
		copy(lockKeys, keys)
		copy(lockKeys[len(keys):], indexKeys)
		if e.skipLocked {
			var skipped map[string]struct{}
			skipped, err = lockKeysSkipLocked(ctx, e.Ctx(), e.waitTime, lockKeys...)
			if err != nil {
				return err
			}
			if len(skipped) > 0 {
				keys, indexKeys, err = e.dropSkipLockedRows(ctx, keys, indexKeys, skipped)
				if err != nil {
					return err
				}
			}
		} else {
			err = LockKeys(ctx, e.Ctx(), e.waitTime, lockKeys...)
			if err != nil {
				return err
			}
		}
	}
	// Fetch all values.
	values, err = batchGetter.BatchGet(ctx, keys)
	if err != nil {
		return err
	}
	handles := make([]kv.Handle, 0, len(values))
	var existKeys []kv.Key
	if e.lock && rc {
		existKeys = make([]kv.Key, 0, 2*len(values))
	}
	// Under RC with skip-locked, track the (row key, index key) pair of each kept row
	// so that rows with a skipped key can be filtered after locking.
	var rcRowKeys, rcIdxKeys []kv.Key
	if e.lock && rc && e.skipLocked {
		rcRowKeys = make([]kv.Key, 0, len(values))
		rcIdxKeys = make([]kv.Key, 0, len(values))
	}
	e.values = make([][]byte, 0, len(values))
	for i, key := range keys {
		val := values[string(key)]
		if val.IsValueEmpty() {
			if e.idxInfo != nil && (!e.tblInfo.IsCommonHandle || !e.idxInfo.Primary) &&
				!e.Ctx().GetSessionVars().StmtCtx.WeakConsistency {
				return (&consistency.Reporter{
					HandleEncode: func(_ kv.Handle) kv.Key {
						return key
					},
					IndexEncode: func(_ *consistency.RecordData) kv.Key {
						return indexKeys[i]
					},
					Tbl:             e.tblInfo,
					Idx:             e.idxInfo,
					EnableRedactLog: e.Ctx().GetSessionVars().EnableRedactLog,
					Storage:         e.Ctx().GetStore(),
				}).ReportLookupInconsistent(ctx,
					1, 0,
					e.handles[i:i+1],
					e.handles,
					[]consistency.RecordData{{}},
				)
			}
			continue
		}
		e.values = append(e.values, val.Value)
		handles = append(handles, e.handles[i])
		if e.lock && rc {
			existKeys = append(existKeys, key)
			// when e.handles is set in builder directly, index should be primary key and the plan is CommonHandleRead
			// with clustered index enabled, indexKeys is empty in this situation
			// lock primary key for clustered index table is redundant
			if len(indexKeys) != 0 {
				existKeys = append(existKeys, indexKeys[i])
			}
			if rcRowKeys != nil {
				rcRowKeys = append(rcRowKeys, key)
				var idxKey kv.Key
				if len(indexKeys) != 0 {
					idxKey = indexKeys[i]
				}
				rcIdxKeys = append(rcIdxKeys, idxKey)
			}
		}
	}
	// Lock exists keys only for Read Committed Isolation.
	if e.lock && rc {
		if e.skipLocked {
			var skipped map[string]struct{}
			skipped, err = lockKeysSkipLocked(ctx, e.Ctx(), e.waitTime, existKeys...)
			if err != nil {
				return err
			}
			if len(skipped) > 0 {
				var rollbackKeys [][]byte
				kept := 0
				for i := range handles {
					_, rowSkipped := skipped[string(rcRowKeys[i])]
					idxSkipped := false
					if rcIdxKeys[i] != nil {
						_, idxSkipped = skipped[string(rcIdxKeys[i])]
					}
					if rowSkipped || idxSkipped {
						// Roll back the lock of the pair's other key: the index lock
						// and the row lock must be treated as one atomic group.
						if !rowSkipped {
							rollbackKeys = append(rollbackKeys, rcRowKeys[i])
						}
						if rcIdxKeys[i] != nil && !idxSkipped {
							rollbackKeys = append(rollbackKeys, rcIdxKeys[i])
						}
						continue
					}
					e.values[kept] = e.values[i]
					handles[kept] = handles[i]
					kept++
				}
				e.values = e.values[:kept]
				handles = handles[:kept]
				if err = e.rollbackSkippedPartnerLocks(ctx, rollbackKeys); err != nil {
					return err
				}
			}
		} else {
			err = LockKeys(ctx, e.Ctx(), e.waitTime, existKeys...)
			if err != nil {
				return err
			}
		}
	}
	e.handles = handles
	return nil
}

// dropSkipLockedRows removes the rows whose row key or index key was skipped by a
// skip-locked lock request, and rolls back the pessimistic lock on the partner key of
// each dropped row: the index lock and the row lock must be treated as one atomic
// group, and a kept lock of a dropped row would block other skip-locked readers.
func (e *BatchPointGetExec) dropSkipLockedRows(
	ctx context.Context, keys, indexKeys []kv.Key, skipped map[string]struct{},
) ([]kv.Key, []kv.Key, error) {
	var rollbackKeys [][]byte
	keptKeys := keys[:0]
	keptHandles := e.handles[:0]
	var keptIdxKeys []kv.Key
	if len(indexKeys) > 0 {
		keptIdxKeys = indexKeys[:0]
	}
	for i, key := range keys {
		_, rowSkipped := skipped[string(key)]
		var idxKey kv.Key
		if len(indexKeys) > 0 {
			idxKey = indexKeys[i]
		}
		idxSkipped := false
		if idxKey != nil {
			_, idxSkipped = skipped[string(idxKey)]
		}
		if rowSkipped || idxSkipped {
			if !rowSkipped {
				rollbackKeys = append(rollbackKeys, key)
			}
			if idxKey != nil && !idxSkipped {
				rollbackKeys = append(rollbackKeys, idxKey)
			}
			continue
		}
		keptKeys = append(keptKeys, key)
		keptHandles = append(keptHandles, e.handles[i])
		if idxKey != nil {
			keptIdxKeys = append(keptIdxKeys, idxKey)
		}
	}
	e.handles = keptHandles
	if err := e.rollbackSkippedPartnerLocks(ctx, rollbackKeys); err != nil {
		return nil, nil, err
	}
	return keptKeys, keptIdxKeys, nil
}

// rollbackSkippedPartnerLocks releases the pessimistic locks this statement acquired
// on keys whose pair key was skipped, and drops their cached values, which were only
// valid under the released locks.
func (e *BatchPointGetExec) rollbackSkippedPartnerLocks(ctx context.Context, rollbackKeys [][]byte) error {
	if len(rollbackKeys) == 0 {
		return nil
	}
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}
	rb, ok := txn.(kv.PessimisticRollbacker)
	if !ok {
		return errors.New("the transaction does not support rolling back pessimistic locks on specific keys")
	}
	if err := rb.PessimisticRollbackKeys(ctx, rollbackKeys); err != nil {
		return err
	}
	txnCtx := e.Ctx().GetSessionVars().TxnCtx
	for _, key := range rollbackKeys {
		txnCtx.UnsetPessimisticLockCache(key)
	}
	return nil
}

// lockKeysSkipLocked locks the keys like LockKeys but with the skip-locked behavior:
// keys locked by other transactions are skipped instead of waited for, and the set of
// skipped keys is returned. Values of skipped keys are never cached.
func lockKeysSkipLocked(ctx context.Context, sctx sessionctx.Context, lockWaitTime int64, keys ...kv.Key) (map[string]struct{}, error) {
	sessVars := sctx.GetSessionVars()

	if err := checkMaxExecutionTimeExceeded(sctx); err != nil {
		return nil, err
	}

	txnCtx := sessVars.TxnCtx
	lctx, err := newLockCtx(sctx, lockWaitTime, len(keys), false)
	if err != nil {
		return nil, err
	}
	lctx.InitSkipLocked(len(keys))
	if txnCtx.IsPessimistic {
		lctx.InitReturnValues(len(keys))
	}
	if err := doLockKeys(ctx, sctx, lctx, keys...); err != nil {
		return nil, err
	}
	skipped := make(map[string]struct{})
	lctx.IterateSkippedKeys(func(key []byte) {
		skipped[string(key)] = struct{}{}
	})
	if txnCtx.IsPessimistic {
		// When doLockKeys returns without error, no other goroutines access the map,
		// it's safe to read it without mutex.
		for _, key := range keys {
			if _, ok := skipped[string(key)]; ok {
				continue
			}
			if v, ok := lctx.GetValueNotLocked(key); ok {
				txnCtx.SetPessimisticLockCache(key, v)
			}
		}
	}
	return skipped, nil
}

// LockKeys locks the keys for pessimistic transaction.
func LockKeys(ctx context.Context, sctx sessionctx.Context, lockWaitTime int64, keys ...kv.Key) error {
	sessVars := sctx.GetSessionVars()

	if err := checkMaxExecutionTimeExceeded(sctx); err != nil {
		return err
	}

	txnCtx := sessVars.TxnCtx
	lctx, err := newLockCtx(sctx, lockWaitTime, len(keys), false)
	if err != nil {
		return err
	}
	if txnCtx.IsPessimistic {
		lctx.InitReturnValues(len(keys))
	}
	err = doLockKeys(ctx, sctx, lctx, keys...)
	if err != nil {
		return err
	}
	if txnCtx.IsPessimistic {
		// When doLockKeys returns without error, no other goroutines access the map,
		// it's safe to read it without mutex.
		for _, key := range keys {
			if v, ok := lctx.GetValueNotLocked(key); ok {
				txnCtx.SetPessimisticLockCache(key, v)
			}
		}
	}
	return nil
}

// PessimisticLockCacheGetter implements the kv.Getter interface.
// It is used as a middle cache to construct the BufferedBatchGetter.
type PessimisticLockCacheGetter struct {
	txnCtx *variable.TransactionContext
}

// Get implements the kv.Getter interface.
func (getter *PessimisticLockCacheGetter) Get(_ context.Context, key kv.Key, options ...kv.GetOption) (kv.ValueEntry, error) {
	if len(options) > 0 {
		var opt tikv.GetOptions
		opt.Apply(options)
		if opt.ReturnCommitTS() {
			return kv.ValueEntry{}, errors.New("WithReturnCommitTS option is not supported for pessimistic lock cacheBatchGetter.Get")
		}
	}

	val, ok := getter.txnCtx.GetKeyInPessimisticLockCache(key)
	if ok {
		return kv.NewValueEntry(val, 0), nil
	}
	return kv.ValueEntry{}, kv.ErrNotExist
}

type cacheBatchGetter struct {
	ctx      sessionctx.Context
	tid      int64
	snapshot kv.Snapshot
}

func (b *cacheBatchGetter) BatchGet(ctx context.Context, keys []kv.Key, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	if len(options) > 0 {
		var opt tikv.BatchGetOptions
		opt.Apply(options)
		if opt.ReturnCommitTS() {
			return nil, errors.New("WithReturnCommitTS option is not supported for pessimistic lock cacheBatchGetter.BatchGet")
		}
	}

	cacheDB := b.ctx.GetStore().GetMemCache()
	vals := make(map[string]kv.ValueEntry)
	for _, key := range keys {
		val, err := cacheDB.UnionGet(ctx, b.tid, b.snapshot, key)
		if err != nil {
			if !kv.ErrNotExist.Equal(err) {
				return nil, err
			}
			continue
		}
		vals[string(key)] = kv.NewValueEntry(val, 0)
	}
	return vals, nil
}

func newCacheBatchGetter(ctx sessionctx.Context, tid int64, snapshot kv.Snapshot) *cacheBatchGetter {
	return &cacheBatchGetter{ctx, tid, snapshot}
}
