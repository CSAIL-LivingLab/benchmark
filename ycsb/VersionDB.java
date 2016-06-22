/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package simpledb.versioned.benchmark.ycsb;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import simpledb.DbException;
import simpledb.Field;
import simpledb.Predicate;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.versioned.benchmark.ycsb.Scenario.VersionedTableEntry;
import simpledb.versioned.benchmark.ycsb.measurements.Measurements;

/**
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the
 * test. This class should be constructed using a no-argument constructor, so we
 * can load it dynamically. Any argument-based initialization should be done by
 * init().
 */
public abstract class VersionDB {

	public static enum MergeProc {
		THREE_WAY("three_way"), TWO_WAY("two_way");

		private final String name;

		MergeProc(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	public static abstract class Transaction {
		String subOperationName;
		String[] branchesOperatedOn;

		public Transaction(String subOperationName, String... branchesOperatedOn) {
			this.subOperationName = subOperationName;
			this.branchesOperatedOn = branchesOperatedOn;
		}

		public Transaction(String[] branchesOperatedOn) {
			this("NONE", branchesOperatedOn);
		}

		public Transaction() {
			this("NONE", new String[] {});
		}

		public abstract void execute(VersionDB db, String tableName);

		public String getMetricString() {
			return "SUB-OPERATION: " + subOperationName;
		}
	}

	public abstract int getLastOpCount();

	public static class TransactionExecutor {
		final VersionDB db;
		final VersionedTableEntry table;
		final String operationName;
		Transaction transaction;

		public TransactionExecutor(VersionDB db, VersionedTableEntry table, String operationName) {
			this.db = db;
			this.table = table;
			this.operationName = operationName;
		}

		public void setTransaction(Transaction transaction) {
			this.transaction = transaction;
		}

		public void doTransaction() {
			long st = System.nanoTime();
			transaction.execute(db, table.tableName);
			long en = System.nanoTime();
			String metricString = getMetricString(table.tableName, table.tableType);
			db.getMeasurements().measure(metricString, (int) ((en - st) / 1000000));
		}

		public String getMetricString(String tableName, String tableType) {
			String out = "TABLE: " + tableName + "," + "TYPE: " + tableType + "," + "OPERATION: " + operationName + ","
					+ transaction.getMetricString() + ", OP COUNT: " + db.getLastOpCount();
			;
			return out;
		}

		@Override
		public String toString() {
			return getMetricString(table.tableName, table.tableType);
		}
	}

	public static interface TupleSrc extends Iterator<Tuple> {
		public void open(TransactionId tid) throws DbException, TransactionAbortedException;

		public void rewind();

		public void close();
	}

	Measurements _measurements;

	public void setMeasurements(Measurements measurements) {
		_measurements = measurements;
	}

	public Measurements getMeasurements() {
		return _measurements;
	}

	/**
	 * Properties for configuring this DB.
	 */
	Properties _p = new Properties();

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p) {
		_p = p;

	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties() {
		return _p;
	}

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	public void init() throws DBException {
	}

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	public void cleanup() throws DBException {
	}

	public abstract void close(String tableName) throws IOException;

	public abstract void force(String tableName) throws IOException;

	public abstract List<String> getVersionDBFileTypes();

	public abstract TransactionId startTransaction();

	public abstract void setNumBufferPoolPages(int numPages);

	public abstract void commitTransaction(TransactionId tid);

	public abstract void abortTransaction(TransactionId tid);

	public abstract void scan(TransactionId tid, Integer[] fields, String tableName, boolean reportContainingVersions,
			String... bids);

	public abstract TupleSrc getTableScanner(TransactionId tid, String... branchNames);

	public abstract void insert(TransactionId tid, String tableName, String branchName, Tuple tup);

	public abstract void update(TransactionId tid, String tableName, String branchName, Tuple tup);

	public abstract void read(TransactionId tid, String tableName, String branchName, Field primaryKey);

	public abstract void delete(TransactionId tid, String tableName, String branchName, Tuple tup);

	public abstract void createVersionedTable(TransactionId tid, String tableName, TupleDesc desc, String type,
			MergeProc mergeProc);

	public abstract void merge(TransactionId tid, String tableName, String[] parents);

	public abstract void branch(TransactionId tid, String tableName, String parentBranchName, String newBranchName);

	public abstract void commit(TransactionId tid, String tableName, String branchName);

	public abstract List<String> getBranches(TransactionId tid, String tableName);

	/**
	 * This is very quick hack to get this working, this should just be a
	 * library and have not randomness.
	 */
	public abstract void randomCheckout(TransactionId tid, String tableName);

	public abstract TupleSrc getNewTupleSrc(TupleDesc td, int numTuples);

	public abstract TupleSrc getTupleSrcFromFile(String fileName, TupleDesc td);

	public abstract void createVersionedTableFromFile(TransactionId tid, String filename, String tableName,
			TupleDesc desc, String type);

	public abstract void copyVersionedTable(TransactionId tid, String tableName, String newTableName);

	public abstract String getVersionMetaDataStringRep(String tableName);

	public abstract void setPageSize(int pageSize);

	public abstract void flushAllPages() throws IOException;

	public abstract void prepareForWorkload(String tableName);

	public abstract long getSize(String tableName);

	public abstract void clearDatabase();

	public abstract void deleteTable(String tableName);

	/**
	 * SELECT * FROM R(V1), R(V2) WHERE R(V1).pKey = R(V2).pKey
	 * 
	 * Note that we don't just want records which are in both V1 and V2 (that's
	 * a quick bitmap operation), but we want any records, possibly updates, in
	 * either branch which have the same tupleId. We therefore need a full hash
	 * join on both versions. Using our full hash index, this becomes just the
	 * intersection of the key sets of both versions.
	 */
	public abstract void QUERY_3(TransactionId tid, String tableName, String branchName1, String branchName2,
			Predicate pred);

	/**
	 * SELECT V FROM R WHERE R.x > alpha
	 * 
	 * The specified field 'x' must be an integer. The field 'x' is assumed to
	 * be a non-indexed field such that a full all-versions scan is required.
	 * 
	 * SELECT V, * FROM R WHERE R.pKey = x
	 * 
	 * XXX: We assume the use of a primary key index. The field 'f' is the value
	 * of the primary key field on which we want to match. The query simply
	 * searches the primary key index and pulls the relevant records (at most
	 * numVersions such records should exist).
	 * 
	 * TODO: Return the version as well? Don't know if just calling Tuple.concat
	 * will work here with the TupleId being hidden in there. Don't need to find
	 * out right now; not really necessary anyway.
	 */
	public abstract void QUERY_4(TransactionId tid, String tableName, int fieldno, int alpha);

	/**
	 * diff
	 * 
	 * Returns two DbFileIterators, one for the positive diff and one for the
	 * negative diff. That is, the positive diff contains records in 'to' but
	 * not in 'from' and the negative diff contains records in 'from' but not in
	 * 'to'.
	 * 
	 * XXX: The positive and negative diff should probably come from two
	 * separate methods so we can pull the positive diff independently for
	 * merging.
	 */
	public abstract void diff(TransactionId tid, String tableName, String fromBranchName, String toBranchName);

}
