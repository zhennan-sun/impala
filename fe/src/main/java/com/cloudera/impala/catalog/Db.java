// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD
import java.util.Set;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.Logger;

import com.cloudera.impala.catalog.Catalog.TableNotFoundException;
import com.google.common.collect.Sets;
=======

import org.apache.log4j.Logger;

import com.cloudera.impala.catalog.Function.CompareMode;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TDatabase;
import com.cloudera.impala.thrift.TFunctionCategory;
import com.cloudera.impala.util.PatternMatcher;
import com.google.common.base.Preconditions;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.google.common.collect.Lists;

/**
 * Internal representation of db-related metadata. Owned by Catalog instance.
 * Not thread safe.
<<<<<<< HEAD
 */
public class Db {
  private static final Logger LOG = Logger.getLogger(Db.class);

  private final String name;

  private final Catalog parentCatalog;
  private final HiveMetaStoreClient client;

  // map from table name to Table
  private final Map<String, Table> tables;

  // If true, table map values are populated lazily on read.
  final boolean lazy;

  private Table loadTable(String tableName) {
    try {
      return Table.load(parentCatalog.getNextTableId(), client, this, tableName);
    } catch (UnsupportedOperationException ex) {
      LOG.warn(ex);
    }

    return null;
  }

  /**
   * Loads all tables in the the table map, forcing removal of any tables that don't
   * load correctly.
   */
  private void forceLoadAllTables() {
    // Need to copy the keyset to avoid concurrent modification exceptions
    // if we try to remove a table in error
    Set<String> keys = Sets.newHashSet(tables.keySet());
    for (String s: keys) {
      tables.get(s);
=======
 *
 * The static initialisation method loadDb is the only way to construct a Db
 * object.
 *
 * Tables are stored in a map from the table name to the table object. They may
 * be loaded 'eagerly' at construction or 'lazily' on first reference.
 * Tables are accessed via getTable which may trigger a metadata read in two cases:
 *  * if the table has never been loaded
 *  * if the table loading failed on the previous attempt
 */
public class Db implements CatalogObject {
  private static final Logger LOG = Logger.getLogger(Db.class);
  private final Catalog parentCatalog_;
  private final TDatabase thriftDb_;
  private long catalogVersion_ = Catalog.INITIAL_CATALOG_VERSION;

  // Table metadata cache.
  private final CatalogObjectCache<Table> tableCache_;

  // All of the registered user functions. The key is the user facing name (e.g. "myUdf"),
  // and the values are all the overloaded variants (e.g. myUdf(double), myUdf(string))
  // This includes both UDFs and UDAs. Updates are made thread safe by synchronizing
  // on this map.
  private final HashMap<String, List<Function>> functions_;

  // If true, this database is an Impala system database.
  // (e.g. can't drop it, can't add tables to it, etc).
  private boolean isSystemDb_ = false;

  public Db(String name, Catalog catalog) {
    thriftDb_ = new TDatabase(name.toLowerCase());
    parentCatalog_ = catalog;
    tableCache_ = new CatalogObjectCache<Table>();
    functions_ = new HashMap<String, List<Function>>();
  }

  public void setIsSystemDb(boolean b) { isSystemDb_ = b; }

  /**
   * Creates a Db object with no tables based on the given TDatabase thrift struct.
   */
  public static Db fromTDatabase(TDatabase db, Catalog parentCatalog) {
    return new Db(db.getDb_name(), parentCatalog);
  }

  public boolean isSystemDb() { return isSystemDb_; }
  public TDatabase toThrift() { return thriftDb_; }
  public String getName() { return thriftDb_.getDb_name(); }
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.DATABASE;
  }

  /**
   * Adds a table to the table cache.
   */
  public void addTable(Table table) {
    tableCache_.add(table);
  }

  /**
   * Gets all table names in the table cache.
   */
  public List<String> getAllTableNames() {
    return Lists.newArrayList(tableCache_.keySet());
  }

  public boolean containsTable(String tableName) {
    return tableCache_.contains(tableName.toLowerCase());
  }

  /**
   * Returns the Table with the given name if present in the table cache or null if the
   * table does not exist in the cache.
   */
  public Table getTable(String tblName) {
    return tableCache_.get(tblName);
  }

  /**
   * Removes the table name and any cached metadata from the Table cache.
   */
  public Table removeTable(String tableName) {
    return tableCache_.remove(tableName.toLowerCase());
  }

  /**
   * Returns the number of functions in this database.
   */
  public int numFunctions() {
    synchronized (functions_) {
      return functions_.size();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }

  /**
<<<<<<< HEAD
   * Extends the usual HashMap to lazily load tables on read (through 'get').
   *
   * Differs from the usual behaviour of Map in that calling get(...) can
   * alter the key set if a lazy load fails (see comments on get, below). If this
   * map is exposed outside of the catalog, make sure to call forceLoadAllTables to
   * make this behave exactly like a usual Map.
   *
   * This class is not thread safe.
   */
  private class LazyTableMap extends HashMap<String, Table> {
    // Required because HashMap implements Serializable
    private static final long serialVersionUID = 1974243714395998559L;

    /**
     * If a table name is present in the map, but its associated value is null,
     * try and load the table from the metastore and populate the map. If loading
     * fails, the table name (the key) is removed from the map to avoid repeatedly
     * trying to load the table - future accesses of that key will still return
     * null.
     */
    @Override
    public Table get(Object key) {
      if (!super.containsKey(key)) {
        return null;
      }

      Table ret = super.get(key);
      if (ret != null) {
        // Already loaded
        return ret;
      }

      ret = loadTable((String)key);
      if (ret == null) {
        remove(key);
      } else {
        put((String)key, ret);
      }
      return ret;
    }
  }

  private Db(String name, Catalog catalog, HiveMetaStoreClient hiveClient,
      boolean lazy) {
    this.name = name;
    this.lazy = lazy;
    this.tables = new LazyTableMap();
    this.parentCatalog = catalog;
    this.client = hiveClient;
  }

  /**
   * Load the metadata of a Hive database into our own
   * in-memory metadata representation.
   * Ignore tables with columns of unsupported types (all complex types).
   *
   * @param client
   *          HiveMetaStoreClient to communicate with Metastore
   * @param dbName
   * @param lazy
   *          if true, tables themselves are loaded lazily on read, otherwise they are
   *          read eagerly in this method. The set of table names is always loaded.
   * @return non-null Db instance (possibly containing no tables)
   */
  public static Db loadDb(Catalog catalog, HiveMetaStoreClient client, String dbName,
      boolean lazy) {
    try {
      Db db = new Db(dbName, catalog, client, lazy);
      List<String> tblNames = null;
      tblNames = client.getTables(dbName, "*");
      for (String s: tblNames) {
        db.tables.put(s, null);
      }

      if (!lazy) {
        db.forceLoadAllTables();
      }

      return db;
    } catch (MetaException e) {
      // turn into unchecked exception
      throw new UnsupportedOperationException(e.toString());
    }
  }

  public String getName() {
    return name;
  }

  public Map<String, Table> getTables() {
    // We need to force all tables to be loaded at this point. Callers of this method
    // assume that only successfully loaded tables will appear in this map, but until
    // we try to load them, we can't be sure.
    if (lazy) {
      forceLoadAllTables();
    }
    return tables;
  }

  public List<String> getAllTableNames() {
    return Lists.newArrayList(tables.keySet());
  }

  /**
   * Case-insensitive lookup
   */
  public Table getTable(String tbl) {
    return tables.get(tbl.toLowerCase());
  }

  /**
   * Forces reload of named table on next access
   */
  public void invalidateTable(String table) throws TableNotFoundException {
    if (tables.containsKey(table)) {
      tables.put(table, null);
    } else {
      throw new TableNotFoundException("Could not invalidate non-existent table: "
          + table);
    }
  }
=======
   * See comment in Catalog.
   */
  public boolean containsFunction(String name) {
    synchronized (functions_) {
      return functions_.get(name) != null;
    }
  }

  /*
   * See comment in Catalog.
   */
  public Function getFunction(Function desc, Function.CompareMode mode) {
    synchronized (functions_) {
      List<Function> fns = functions_.get(desc.functionName());
      if (fns == null) return null;

      // First check for identical
      for (Function f: fns) {
        if (f.compare(desc, Function.CompareMode.IS_IDENTICAL)) return f;
      }
      if (mode == Function.CompareMode.IS_IDENTICAL) return null;

      // Next check for indistinguishable
      for (Function f: fns) {
        if (f.compare(desc, Function.CompareMode.IS_INDISTINGUISHABLE)) return f;
      }
      if (mode == Function.CompareMode.IS_INDISTINGUISHABLE) return null;

      // Finally check for is_subtype
      for (Function f: fns) {
        if (f.compare(desc, Function.CompareMode.IS_SUPERTYPE_OF)) return f;
      }
    }
    return null;
  }

  public Function getFunction(String signatureString) {
    synchronized (functions_) {
      for (List<Function> fns: functions_.values()) {
        for (Function f: fns) {
          if (f.signatureString().equals(signatureString)) return f;
        }
      }
    }
    return null;
  }

  /**
   * See comment in Catalog.
   */
  public boolean addFunction(Function fn) {
    Preconditions.checkState(fn.dbName().equals(getName()));
    // TODO: add this to persistent store
    synchronized (functions_) {
      if (getFunction(fn, Function.CompareMode.IS_INDISTINGUISHABLE) != null) {
        return false;
      }
      List<Function> fns = functions_.get(fn.functionName());
      if (fns == null) {
        fns = Lists.newArrayList();
        functions_.put(fn.functionName(), fns);
      }
      return fns.add(fn);
    }
  }

  /**
   * See comment in Catalog.
   */
  public Function removeFunction(Function desc) {
    // TODO: remove this from persistent store.
    synchronized (functions_) {
      Function fn = getFunction(desc, Function.CompareMode.IS_INDISTINGUISHABLE);
      if (fn == null) return null;
      List<Function> fns = functions_.get(desc.functionName());
      Preconditions.checkNotNull(fns);
      fns.remove(fn);
      if (fns.isEmpty()) functions_.remove(desc.functionName());
      return fn;
    }
  }

  /**
   * Removes a Function with the matching signature string. Returns the removed Function
   * if a Function was removed as a result of this call, null otherwise.
   * TODO: Move away from using signature strings and instead use Function IDs.
   */
  public Function removeFunction(String signatureStr) {
    synchronized (functions_) {
      Function targetFn = getFunction(signatureStr);
      if (targetFn != null) return removeFunction(targetFn);
    }
    return null;
  }

  /**
   * Add a builtin with the specified name and signatures to this db.
   * This defaults to not using a Prepare/Close function.
   */
  public void addScalarBuiltin(String fnName, String symbol, boolean varArgs,
      Type retType, Type ... args) {
    addScalarBuiltin(fnName, symbol, null, null, varArgs, retType, args);
  }

  /**
   * Add a builtin with the specified name and signatures to this db.
   */
  public void addScalarBuiltin(String fnName, String symbol, String prepareFnSymbol,
      String closeFnSymbol, boolean varArgs, Type retType, Type ... args) {
    Preconditions.checkState(isSystemDb());
    addBuiltin(ScalarFunction.createBuiltin(
        fnName, Lists.newArrayList(args), varArgs, retType,
        symbol, prepareFnSymbol, closeFnSymbol, false));
  }

  /**
   * Adds a builtin to this database. The function must not already exist.
   */
  public void addBuiltin(Function fn) {
    Preconditions.checkState(isSystemDb());
    Preconditions.checkState(fn != null);
    Preconditions.checkState(getFunction(fn, CompareMode.IS_IDENTICAL) == null);
    addFunction(fn);
  }

  /**
   * Returns a map of functionNames to list of (overloaded) functions with that name.
   * This is not thread safe so a higher level lock must be taken while iterating
   * over the returned functions.
   */
  protected HashMap<String, List<Function>> getAllFunctions() {
    return functions_;
  }

  /**
   * Returns all functions that match 'fnPattern'.
   */
  public List<Function> getFunctions(TFunctionCategory category,
      PatternMatcher fnPattern) {
    List<Function> functions = Lists.newArrayList();
    synchronized (functions_) {
      for (Map.Entry<String, List<Function>> fns: functions_.entrySet()) {
        if (fnPattern.matches(fns.getKey())) {
          for (Function fn: fns.getValue()) {
            if (!fn.userVisible()) continue;
            if (category == null
                || (category == TFunctionCategory.SCALAR && fn instanceof ScalarFunction)
                || (category == TFunctionCategory.AGGREGATE
                    && fn instanceof AggregateFunction
                    && ((AggregateFunction)fn).isAggregateFn())
                || (category == TFunctionCategory.ANALYTIC
                    && fn instanceof AggregateFunction
                    && ((AggregateFunction)fn).isAnalyticFn())) {
              functions.add(fn);
            }
          }
        }
      }
    }
    return functions;
  }

  @Override
  public long getCatalogVersion() { return catalogVersion_; }
  @Override
  public void setCatalogVersion(long newVersion) { catalogVersion_ = newVersion; }
  public Catalog getParentCatalog() { return parentCatalog_; }

  @Override
  public boolean isLoaded() { return true; }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
