// Copyright 2021 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestBatchPointGetLockExistKey(t *testing.T) {
	var wg sync.WaitGroup
	errCh := make(chan error)
	store := testkit.CreateMockStore(t)

	testLock := func(rc bool, key string, tableName string) {
		doneCh := make(chan struct{}, 1)
		tk1, tk2 := testkit.NewTestKit(t, store), testkit.NewTestKit(t, store)

		errCh <- tk1.ExecToErr("use test")
		errCh <- tk2.ExecToErr("use test")
		tk1.Session().GetSessionVars().EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly

		errCh <- tk1.ExecToErr(fmt.Sprintf("drop table if exists %s", tableName))
		errCh <- tk1.ExecToErr(fmt.Sprintf("create table %s(id int, v int, k int, %s key0(id, v))", tableName, key))
		errCh <- tk1.ExecToErr(fmt.Sprintf("insert into %s values(1, 1, 1), (2, 2, 2)", tableName))

		if rc {
			errCh <- tk1.ExecToErr("set tx_isolation = 'READ-COMMITTED'")
			errCh <- tk2.ExecToErr("set tx_isolation = 'READ-COMMITTED'")
		}

		errCh <- tk1.ExecToErr("begin pessimistic")
		errCh <- tk2.ExecToErr("begin pessimistic")

		// select for update
		if !rc {
			// lock exist key only for repeatable read
			errCh <- tk1.ExecToErr(fmt.Sprintf("select * from %s where (id, v) in ((1, 1), (2, 2)) for update", tableName))
		} else {
			// read committed will not lock non-exist key
			errCh <- tk1.ExecToErr(fmt.Sprintf("select * from %s where (id, v) in ((1, 1), (2, 2), (3, 3)) for update", tableName))
		}
		errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(3, 3, 3)", tableName))
		go func() {
			errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(1, 1, 10)", tableName))
			doneCh <- struct{}{}
		}()

		time.Sleep(150 * time.Millisecond)
		errCh <- tk1.ExecToErr(fmt.Sprintf("update %s set v = 2 where id = 1 and v = 1", tableName))

		errCh <- tk1.ExecToErr("commit")
		<-doneCh
		errCh <- tk2.ExecToErr("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 2 2",
			"3 3 3",
			"1 1 10",
		))

		// update
		errCh <- tk1.ExecToErr("begin pessimistic")
		errCh <- tk2.ExecToErr("begin pessimistic")
		if !rc {
			// lock exist key only for repeatable read
			errCh <- tk1.ExecToErr(fmt.Sprintf("update %s set v = v + 1 where (id, v) in ((2, 2), (3, 3))", tableName))
		} else {
			// read committed will not lock non-exist key
			errCh <- tk1.ExecToErr(fmt.Sprintf("update %s set v = v + 1 where (id, v) in ((2, 2), (3, 3), (4, 4))", tableName))
		}
		errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(4, 4, 4)", tableName))
		go func() {
			errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(3, 3, 30)", tableName))
			doneCh <- struct{}{}
		}()
		time.Sleep(150 * time.Millisecond)
		errCh <- tk1.ExecToErr("commit")
		<-doneCh
		errCh <- tk2.ExecToErr("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 3 2",
			"3 4 3",
			"1 1 10",
			"4 4 4",
			"3 3 30",
		))

		// delete
		errCh <- tk1.ExecToErr("begin pessimistic")
		errCh <- tk2.ExecToErr("begin pessimistic")
		if !rc {
			// lock exist key only for repeatable read
			errCh <- tk1.ExecToErr(fmt.Sprintf("delete from %s where (id, v) in ((3, 4), (4, 4))", tableName))
		} else {
			// read committed will not lock non-exist key
			errCh <- tk1.ExecToErr(fmt.Sprintf("delete from %s where (id, v) in ((3, 4), (4, 4), (5, 5))", tableName))
		}
		errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(5, 5, 5)", tableName))
		go func() {
			errCh <- tk2.ExecToErr(fmt.Sprintf("insert into %s values(4, 4,40)", tableName))
			doneCh <- struct{}{}
		}()
		time.Sleep(150 * time.Millisecond)
		errCh <- tk1.ExecToErr("commit")
		<-doneCh
		errCh <- tk2.ExecToErr("commit")
		tk1.MustQuery(fmt.Sprintf("select * from %s", tableName)).Check(testkit.Rows(
			"1 2 1",
			"2 3 2",
			"1 1 10",
			"3 3 30",
			"5 5 5",
			"4 4 40",
		))
		wg.Done()
	}

	for i, one := range []struct {
		rc  bool
		key string
	}{
		{rc: false, key: "primary key"},
		{rc: false, key: "unique key"},
		{rc: true, key: "primary key"},
		{rc: true, key: "unique key"},
	} {
		wg.Add(1)
		tableName := fmt.Sprintf("t_%d", i)
		go testLock(one.rc, one.key, tableName)
	}

	// should works for common handle in clustered index
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id varchar(40) primary key)")
	tk.MustExec("insert into t values('1'), ('2')")
	tk.MustExec("set tx_isolation = 'READ-COMMITTED'")
	tk.MustExec("begin pessimistic")
	tk.MustExec("select * from t where id in('1', '2') for update")
	tk.MustExec("commit")

	go func() {
		wg.Wait()
		close(errCh)
	}()
	for err := range errCh {
		require.NoError(t, err)
	}
}

func TestCacheSnapShot(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	ctx := context.Background()
	txn, err := se.GetStore().Begin(tikv.WithStartTS(0))
	memBuffer := txn.GetMemBuffer()
	require.NoError(t, err)
	keys := make([]kv.Key, 0, 2)
	for i := range 2 {
		keys = append(keys, []byte(string(rune(i))))
	}
	err = memBuffer.Set(keys[0], []byte("1111"))
	require.NoError(t, err)
	err = memBuffer.Set(keys[1], []byte("2222"))
	require.NoError(t, err)
	cacheTableSnapShot := executor.MockNewCacheTableSnapShot(nil, memBuffer)
	get, err := cacheTableSnapShot.Get(ctx, keys[0])
	require.NoError(t, err)
	require.Equal(t, get, kv.NewValueEntry([]byte("1111"), 0))
	batchGet, err := cacheTableSnapShot.BatchGet(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, batchGet[string(keys[0])], kv.NewValueEntry([]byte("1111"), 0))
	require.Equal(t, batchGet[string(keys[1])], kv.NewValueEntry([]byte("2222"), 0))
}

func TestPointGetForTemporaryTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create global temporary table t1 (id int primary key, val int) on commit delete rows")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (1,1)")
	tk.MustQuery("explain format = 'brief' select * from t1 where id in (1, 2, 3)").
		Check(testkit.Rows("Batch_Point_Get 3.00 root table:t1 handle:[1 2 3], keep order:false, desc:false"))

	isV2, _ := infoschema.IsV2(dom.InfoSchema())
	if isV2 {
		t.Skip("This test can not run under infoschema v2, because the later would always visit network")
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy"))
	}()

	// Batch point get.
	tk.MustQuery("select * from t1 where id in (1, 2, 3)").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t1 where id in (2, 3)").Check(testkit.Rows())

	// Point get.
	tk.MustQuery("select * from t1 where id = 1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t1 where id = 2").Check(testkit.Rows())
}

func TestSelectForUpdateSkipLocked(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("create table t (id int primary key, uid varchar(20), c int, unique key uk(uid), key k(c))")
	tk1.MustExec("insert into t values (1, 'u1', 10), (2, 'u2', 10), (3, 'u3', 10)")

	// The feature is gated behind a sysvar and fails loudly when disabled.
	tk2.MustExec("begin pessimistic")
	tk2.MustContainErrMsg("select id from t where c = 10 for update skip locked",
		"This version of TiDB doesn't yet support 'SKIP LOCKED'")
	tk2.MustExec("rollback")

	tk1.MustExec("set session tidb_enable_select_skip_locked = 1")
	tk2.MustExec("set session tidb_enable_select_skip_locked = 1")

	// Multi-table FOR UPDATE SKIP LOCKED is not supported in v1.
	tk2.MustExec("begin pessimistic")
	tk2.MustContainErrMsg("select * from t t1, t t2 where t1.id = t2.id for update skip locked",
		"SKIP LOCKED on multi-table SELECT FOR UPDATE")
	tk2.MustExec("rollback")

	// Index scan + SelectLockExec: rows locked by the other transaction are skipped,
	// while snapshot reads still see them.
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk1.MustQuery("select id from t where id = 2 for update").Check(testkit.Rows("2"))
	tk2.MustQuery("select id from t where c = 10 order by id for update skip locked").
		Check(testkit.Rows("1", "3"))
	tk2.MustQuery("select id from t order by id").Check(testkit.Rows("1", "2", "3"))
	// The unskipped rows are really locked by tk2 now.
	tk1.MustContainErrMsg("select id from t where id = 1 for update nowait", "3572")
	tk1.MustExec("commit")
	tk2.MustExec("commit")

	// PointGet on the clustered PK: a skipped row yields an empty result set.
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk1.MustQuery("select id from t where id = 1 for update").Check(testkit.Rows("1"))
	tk2.MustQuery("select id from t where id = 1 for update skip locked").Check(testkit.Rows())
	tk2.MustQuery("select id from t where id = 2 for update skip locked").Check(testkit.Rows("2"))
	tk1.MustExec("commit")
	tk2.MustExec("commit")

	// BatchPointGet: only the unlocked rows are returned.
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk1.MustQuery("select id from t where id = 2 for update").Check(testkit.Rows("2"))
	tk2.MustQuery("select id from t where id in (1, 2, 3) order by id for update skip locked").
		Check(testkit.Rows("1", "3"))
	tk1.MustExec("commit")
	tk2.MustExec("commit")

	// PointGet via a unique index whose row key is locked by another transaction: the
	// row is skipped and the acquired index-key lock is released again, so after tk1
	// commits, a third session can lock the row through the index without waiting.
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk1.MustQuery("select id from t where id = 3 for update").Check(testkit.Rows("3"))
	tk2.MustQuery("select id from t where uid = 'u3' for update skip locked").Check(testkit.Rows())
	tk1.MustExec("commit")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")
	// The index-key rollback is asynchronous.
	require.Eventually(t, func() bool {
		tk3.MustExec("begin pessimistic")
		defer tk3.MustExec("rollback")
		return tk3.ExecToErr("select id from t where uid = 'u3' for update nowait") == nil
	}, 3*time.Second, 100*time.Millisecond)
	tk2.MustExec("commit")

	// Skip-locked composes with fair locking by exiting fair locking mode.
	tk2.MustExec("set session tidb_pessimistic_txn_fair_locking = 1")
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk1.MustQuery("select id from t where id = 1 for update").Check(testkit.Rows("1"))
	tk2.MustQuery("select id from t where id = 1 for update skip locked").Check(testkit.Rows())
	tk1.MustExec("commit")
	tk2.MustExec("commit")
	tk2.MustExec("set session tidb_pessimistic_txn_fair_locking = default")

	// Read committed isolation: the skipped row is filtered as well.
	tk2.MustExec("set session transaction isolation level read committed")
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk1.MustQuery("select id from t where id = 1 for update").Check(testkit.Rows("1"))
	tk2.MustQuery("select id from t where id = 1 for update skip locked").Check(testkit.Rows())
	tk2.MustQuery("select id from t where id in (1, 2) order by id for update skip locked").
		Check(testkit.Rows("2"))
	tk1.MustExec("commit")
	tk2.MustExec("commit")
}

func TestSelectForUpdateSkipLockedWithOfClause(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("set session tidb_enable_select_skip_locked = 1")
	tk2.MustExec("set session tidb_enable_select_skip_locked = 1")

	tk1.MustExec("drop view if exists ofc_view")
	tk1.MustExec("drop table if exists ofc_ti, ofc_job, ofc_run, ofc_part")
	tk1.MustExec("create table ofc_ti (id int primary key, job_id int, run_id int)")
	tk1.MustExec("create table ofc_job (id int primary key)")
	tk1.MustExec("create table ofc_run (id int primary key)")
	tk1.MustExec("insert into ofc_job values (1)")
	tk1.MustExec("insert into ofc_run values (1)")
	tk1.MustExec("insert into ofc_ti values (1, 1, 1), (2, 1, 1), (3, 1, 1)")

	const claim = "select ofc_ti.id from ofc_ti " +
		"join ofc_job on ofc_job.id = ofc_ti.job_id " +
		"join ofc_run on ofc_run.id = ofc_ti.run_id " +
		"order by ofc_ti.id for update of ofc_ti skip locked"

	// A join locking one table is served: rows locked by the other transaction are
	// skipped, and the tables not named in OF stay unlocked.
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk1.MustQuery("select id from ofc_ti where id = 2 for update").Check(testkit.Rows("2"))
	tk2.MustQuery(claim).Check(testkit.Rows("1", "3"))
	// Probed from tk1, so the claim's own locks cannot satisfy them.
	tk1.MustQuery("select id from ofc_job where id = 1 for update nowait").Check(testkit.Rows("1"))
	tk1.MustQuery("select id from ofc_run where id = 1 for update nowait").Check(testkit.Rows("1"))
	tk1.MustExec("commit")
	tk2.MustExec("commit")

	// The rows the claim did not skip are really locked.
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk1.MustQuery(claim).Check(testkit.Rows("1", "2", "3"))
	tk2.MustContainErrMsg("select id from ofc_ti where id = 1 for update nowait", "3572")
	tk2.MustQuery(claim).Check(testkit.Rows())
	tk1.MustExec("commit")
	tk2.MustExec("commit")

	// An alias in OF refers to the aliased occurrence, as MySQL allows.
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select t.id from ofc_ti t join ofc_job on ofc_job.id = t.job_id " +
		"order by t.id for update of t skip locked").Check(testkit.Rows("1", "2", "3"))
	tk1.MustExec("commit")

	// A plain FOR UPDATE OF locks only the named table, as it did before the guard
	// learned to read the OF list.
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("begin pessimistic")
	tk1.MustQuery("select ofc_ti.id from ofc_ti join ofc_job on ofc_job.id = ofc_ti.job_id " +
		"order by ofc_ti.id for update of ofc_ti").Check(testkit.Rows("1", "2", "3"))
	tk2.MustQuery("select id from ofc_job where id = 1 for update nowait").Check(testkit.Rows("1"))
	tk2.MustContainErrMsg("select id from ofc_ti where id = 1 for update nowait", "3572")
	tk1.MustExec("commit")
	tk2.MustExec("commit")

	// On a prepared-plan-cache hit buildSelect does not re-run, so filterLockTableKeys
	// narrows nothing and only the plan carries the OF list. The second execution must
	// still leave the unnamed table alone.
	tk1.MustExec("prepare claim_of from 'select ofc_ti.id from ofc_ti " +
		"join ofc_job on ofc_job.id = ofc_ti.job_id order by ofc_ti.id for update of ofc_ti'")
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("execute claim_of").Check(testkit.Rows("1", "2", "3"))
	tk1.MustExec("commit")
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("execute claim_of").Check(testkit.Rows("1", "2", "3"))
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk2.MustExec("begin pessimistic")
	tk2.MustQuery("select id from ofc_job where id = 1 for update nowait").Check(testkit.Rows("1"))
	tk2.MustContainErrMsg("select id from ofc_ti where id = 1 for update nowait", "3572")
	tk1.MustExec("commit")
	tk2.MustExec("commit")

	// Locking more than one table still needs per-output-row lock groups.
	tk1.MustContainErrMsg("select ofc_ti.id from ofc_ti join ofc_job on ofc_job.id = ofc_ti.job_id "+
		"for update skip locked", "SKIP LOCKED on multi-table SELECT FOR UPDATE")
	tk1.MustContainErrMsg("select ofc_ti.id from ofc_ti join ofc_job on ofc_job.id = ofc_ti.job_id "+
		"for update of ofc_ti, ofc_job skip locked", "SKIP LOCKED on multi-table SELECT FOR UPDATE")

	// A subquery's OF clause says nothing about what the outer clause locks, in either
	// direction: it must not excuse an outer statement that names no table, nor reject
	// an outer statement that names one.
	tk1.MustContainErrMsg("select ofc_ti.id from ofc_ti join ofc_job on ofc_job.id = ofc_ti.job_id "+
		"where ofc_ti.id in (select j.id from ofc_job j for update of j) "+
		"for update skip locked", "SKIP LOCKED on multi-table SELECT FOR UPDATE")
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select ofc_ti.id from ofc_ti join ofc_job on ofc_job.id = ofc_ti.job_id " +
		"where ofc_ti.id in (select j.id from ofc_job j for update of j) " +
		"order by ofc_ti.id for update of ofc_ti skip locked").Check(testkit.Rows("1"))
	tk1.MustExec("commit")

	// A name that resolves to nothing is still rejected by the locking clause itself.
	tk1.MustContainErrMsg("select ofc_ti.id from ofc_ti join ofc_job on ofc_job.id = ofc_ti.job_id "+
		"for update of nosuch skip locked", "nosuch")

	// A view resolves to a table ID the plan tracks no handle for, so narrowing would
	// empty the map and the count would fall to zero. The join stays rejected.
	tk1.MustExec("create view ofc_view as select id, job_id, run_id from ofc_ti")
	tk1.MustContainErrMsg("select ofc_view.id from ofc_view "+
		"join ofc_job on ofc_job.id = ofc_view.job_id "+
		"join ofc_run on ofc_run.id = ofc_view.run_id "+
		"for update of ofc_view skip locked", "SKIP LOCKED on multi-table SELECT FOR UPDATE")

	// A partitioned table named in OF is rejected: narrowing the count would accept a
	// statement whose keys carry partition IDs the OF list cannot match, locking nothing.
	tk1.MustExec("create table ofc_part (id int primary key, job_id int) " +
		"partition by range (id) (partition p0 values less than (10), " +
		"partition p1 values less than (100))")
	tk1.MustExec("insert into ofc_part values (1, 1), (50, 1)")
	tk1.MustContainErrMsg("select ofc_part.id from ofc_part join ofc_job on ofc_job.id = ofc_part.job_id "+
		"for update of ofc_part skip locked", "SKIP LOCKED on a partitioned table")
	// Without OF there is nothing to narrow, so the single-table form still locks.
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select id from ofc_part order by id for update skip locked").Check(testkit.Rows("1", "50"))
	tk2.MustExec("begin pessimistic")
	tk2.MustContainErrMsg("select id from ofc_part where id = 1 for update nowait", "3572")
	tk1.MustExec("commit")
	tk2.MustExec("commit")
}
