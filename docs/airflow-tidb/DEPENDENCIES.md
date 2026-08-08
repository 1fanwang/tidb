# Pinned dependencies for the Airflow-compatible build

This fork carries `SELECT ... FOR UPDATE SKIP LOCKED` support so Apache Airflow can run on TiDB.
The feature is not in upstream TiDB; it comes from work by @takaidohigasi that is still under
review (pingcap/tidb#69782, pingcap/tidb#69783, tracking issue takaidohigasi/tidb-dev#1).

## Why the dependencies are pinned to our own forks

The feature spans three repositories. Building straight from the author's branches would leave us
exposed: those branches are unmerged and will be rebased or force-pushed as review proceeds, and a
branch can be renamed or deleted outright. Any of those breaks our build with no warning, and a
rebase could silently change what we ship.

So each dependency is forked into this org and tagged at the exact commit we validated. Tags in
our own repositories cannot be moved by anyone else.

| module | upstream source | pinned commit | tag in our fork |
|---|---|---|---|
| `github.com/pingcap/kvproto` | `takaidohigasi/kvproto@skip-locked-protocol` | `24e40a8a16d23d498c535593bdbb906ce07c4d9d` | `airflow-tidb-pin-20260807` |
| `github.com/tikv/client-go/v2` | `takaidohigasi/client-go@skip-locked` | `5663ad14bfd6090457f22a0a99d4fdfe1634ca58` | `airflow-tidb-pin-20260807` |
| TiDB base | `takaidohigasi/tidb@feature/skip-locked` | `8dc60657f4a3e435e1e12174d27970b00a18ede8` | branch base of `skip-locked-of-clause` |

The `replace` directives in `go.mod` point at the forks by pseudo-version, which encodes the
commit hash, so a build either resolves that exact tree or fails loudly.

## Picking up upstream changes

Re-point the `replace` lines at the author's branch, validate, then cut a new
`airflow-tidb-pin-<date>` tag from the commit that passed. Never leave `go.mod` pointing at a
branch name.

## Runtime

The feature is off by default. Enable it per session or globally:

```sql
SET GLOBAL tidb_enable_select_skip_locked = ON;
```

Verify a build actually has it - this returns rows for one session and none for the other, with no
overlap:

```sql
-- session A
BEGIN; SELECT id FROM q WHERE owner IS NULL FOR UPDATE SKIP LOCKED;
-- session B, concurrently
BEGIN; SELECT id FROM q WHERE owner IS NULL FOR UPDATE SKIP LOCKED;
```
