# SortMeRNA macOS Build Failure: RocksDB API Break

## Problem

SortMeRNA fails to compile on macOS when linked against RocksDB >= 11.0.
The GitHub Actions `macos-latest` runner installs RocksDB 11.0.4 via
Homebrew, which contains a breaking API change.

**Error:**

```
ext/sortmerna/src/sortmerna/kvdb.cpp:54:22: error: no matching function for call to 'Open'
   54 |         rocksdb::Status s = rocksdb::DB::Open(options, kvdbPath, &kvdb);
```

**Cause:** RocksDB 11.0 changed the `DB::Open` signature. The third
argument changed from `DB**` (raw pointer-to-pointer) to
`std::unique_ptr<DB>*` (pointer to unique_ptr):

```cpp
// RocksDB <= 10.x (what SortMeRNA expects)
static Status Open(const Options& options, const std::string& name,
                   DB** dbptr);

// RocksDB >= 11.0 (what Homebrew now ships)
static Status Open(const Options& options, const std::string& name,
                   std::unique_ptr<DB>* dbptr);
```

## Affected files

- `ext/sortmerna/include/kvdb.hpp` — line 53: declares `rocksdb::DB* kvdb`
- `ext/sortmerna/src/sortmerna/kvdb.cpp` — line 54: passes `&kvdb` to `Open`

## Required changes

### 1. Change the member variable type (kvdb.hpp)

```diff
 class KeyValueDatabase {
 public:
     KeyValueDatabase(std::string const &kvdbPath);
-    ~KeyValueDatabase() { delete kvdb; }
+    ~KeyValueDatabase() = default;  // unique_ptr handles cleanup
 
     void put(std::string key, std::string val);
     std::string get(std::string key);
     int clear(std::string dbPath);
 private:
-    rocksdb::DB* kvdb;
+    std::unique_ptr<rocksdb::DB> kvdb;
     rocksdb::Options options;
 };
```

### 2. Update call sites (kvdb.cpp)

`DB::Open` now writes into a `std::unique_ptr<DB>` directly — the
existing call `Open(options, kvdbPath, &kvdb)` will work once `kvdb` is
a `unique_ptr`.

`kvdb->Put(...)` and `kvdb->Get(...)` calls continue to work unchanged
(`unique_ptr` supports `->` just like a raw pointer).

### 3. Remove explicit delete

The destructor `~KeyValueDatabase() { delete kvdb; }` must be replaced
with `= default` (or removed entirely). `unique_ptr` handles deallocation
automatically. Double-free would occur if `delete` is called on the
managed pointer.

## Backward compatibility

The `unique_ptr` API was added in RocksDB 11.0. To support both old and
new RocksDB versions, use a version check:

```cpp
#include <rocksdb/version.h>

#if ROCKSDB_MAJOR >= 11
    std::unique_ptr<rocksdb::DB> kvdb;
#else
    rocksdb::DB* kvdb;
#endif
```

And in the destructor:

```cpp
#if ROCKSDB_MAJOR >= 11
    ~KeyValueDatabase() = default;
#else
    ~KeyValueDatabase() { delete kvdb; }
#endif
```

However, if SortMeRNA's miint fork is the only consumer and you control
the minimum RocksDB version, the simpler approach is to require
RocksDB >= 11.0 and use `unique_ptr` unconditionally.

## CI context

- **Ubuntu CI**: installs `librocksdb-dev` from apt, which is currently
  RocksDB 9.x (works fine with the old API)
- **macOS CI**: installs via `brew install rocksdb`, which is 11.0.4
  (breaks with the old API)

The fix should target the SortMeRNA submodule (`ext/sortmerna`, branch
`v4.4.0-miint`). Once merged there, GPL-boundary CI will pass on both
platforms without changes to `build.rs`.

## Scope

This is a two-line change in the SortMeRNA submodule (member type +
destructor). No changes needed in GPL-boundary's Rust code, build
system, or FFI bindings — the `smr_api.h` C interface is not affected.
