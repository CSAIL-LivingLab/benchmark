package simpledb.versioned.benchmark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;

import simpledb.Constants;
import simpledb.Field;
import simpledb.IntField;
import simpledb.Predicate;
import simpledb.TransactionId;
import simpledb.TupleDesc;
import simpledb.Type;
import simpledb.versioned.VersionedTuple;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.BranchOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.CompareOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.DeleteOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.InsertOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.MergeOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.ReadOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.ScanOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.UpdateOpData;
import simpledb.versioned.benchmark.tupleloadstrategy.TupleLoadStrategy;
import simpledb.versioned.benchmark.ycsb.Client;
import simpledb.versioned.benchmark.ycsb.Scenario;
import simpledb.versioned.benchmark.ycsb.Scenario.VersionedTableEntry;
import simpledb.versioned.benchmark.ycsb.VersionDB;
import simpledb.versioned.benchmark.ycsb.VersionDB.Transaction;
import simpledb.versioned.benchmark.ycsb.VersionDB.TransactionExecutor;
import simpledb.versioned.benchmark.ycsb.Workload;
import simpledb.versioned.benchmark.ycsb.WorkloadException;

public class CoreVersionWorkload extends Workload {
	static final Logger logger = Logger.getLogger(CoreVersionWorkload.class);

	public static final String TUPLE_SRC_TRANSACTION_PROPERTY = "tuple_src_transaction";

	/**
	 * Supported queries.
	 */

	public static final String NUM_UPDATES_PROPERTY = "update";
	public static final String NUM_UPDATES_PROPERTY_DEFAULT = "0";

	public static final String NUM_INSERTS_PROPERTY = "insert";
	public static final String NUM_INSERTS_PROPERTY_DEFAULT = "0";

	public static final String NUM_DELETES_PROPERTY = "delete";
	public static final String NUM_DELETES_PROPERTY_DEFAULT = "0";

	public static final String NUM_READS_PROPERTY = "read";
	public static final String NUM_READS_PROPERTY_DEFAULT = "0";

	public static final String NUM_BRANCHES_PROPERTY = "branch";
	public static final String NUM_BRANCHES_PROPERTY_DEFAULT = "0";

	public static final String NUM_CHECKOUTS_PROPERTY = "checkout";
	public static final String NUM_CHECKOUTS_PROPERTY_DEFAULT = "0";

	public static final String NUM_MERGES_PROPERTY = "merge";
	public static final String NUM_MERGES_PROPERTY_DEFAULT = "0";

	public static final String NUM_SCANS_PROPERTY = "scan";
	public static final String NUM_SCANS_PROPERTY_DEFAULT = "0";

	public static final String REPORT_CONTAINING_VERSIONS_PROPERTY = "report_containing_versions";
	public static final String REPORT_CONTAINING_VERSIONS_PROPERTY_DEFAULT = "false";

	public static final String NUM_QUERY_3_PROPERTY = "query3";
	public static final String NUM_QUERY_3_PROPERTY_DEFAULT = "0";

	public static final String NUM_QUERY_4_PROPERTY = "query4";
	public static final String NUM_QUERY_4_PROPERTY_DEFAULT = "0";

	public static final String NUM_DIFF_PROPERTY = "diff";
	public static final String NUM_DIFF_PROPERTY_DEFAULT = "0";

	public static final String NUM_COMMITS_PROPERTY = "commit";

	/**
	 * The name of the property for the scan length distribution. Options are
	 * "uniform" and "zipfian" (favoring short scans)
	 */
	public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

	/**
	 * The default max scan length.
	 */
	public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

	/**
	 * Reporting containing versions for scans. Default is false; set below.
	 */
	public final boolean rcv = false;

	/**
	 * Scenario data.
	 */
	String rootBranchName;

	VersionedTableEntry table;

	TupleDesc td;

	Type primaryKeyType;

	// should be thread local
	VersionedTuple tup;

	/**
	 * Workload generators and selectors.
	 */

	List<String> operations;
	Iterator<String> operationsIt;

	BranchStrategy branchStrategy;

	TupleLoadStrategy tupleLoadStrategy;

	Random opChooser;

	public CoreVersionWorkload() {
		try {
			opChooser = new Random(
					Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.CORE_V_WORKLOAD_SEED_MUL);
		} catch (Exception ex) {
			throw new RuntimeException();
		}
	}

	/**
	 * Initialize the scenario. Called once, in the main client thread, before
	 * any operations are started.
	 */
	@Override
	public void init(Properties p, Scenario scenario) throws WorkloadException {
		table = scenario.getVersionTableEntry();
		td = scenario.getTupleDesc();
		tup = new VersionedTuple(td);
		rootBranchName = scenario.getRootBranchName();

		// setup branch space for transactions
		branchStrategy = scenario.getBranchStrategy();

		// get tuple load strategy
		tupleLoadStrategy = scenario.getTupleLoadStrategy();

		// setup transactions
		operations = new ArrayList<String>();
		setupTransactionDistribution(p);
		operationsIt = operations.iterator();
	}

	/**
	 * Initialize any state for a particular client thread. Since the scenario
	 * object will be shared among all threads, this is the place to create any
	 * state that is specific to one thread. To be clear, this means the
	 * returned object should be created anew on each call to initThread(); do
	 * not return the same object multiple times. The returned object will be
	 * passed to invocations of doInsert() and doTransaction() for this thread.
	 * There should be no side effects from this call; all state should be
	 * encapsulated in the returned object. If you have no state to retain for
	 * this thread, return null. (But if you have no state to retain for this
	 * thread, probably you don't need to override initThread().)
	 * 
	 * @return false if the workload knows it is done for this thread. Client
	 *         will terminate the thread. Return true otherwise. Return true for
	 *         workloads that rely on operationcount. For workloads that read
	 *         traces from a file, return true when there are more to do, false
	 *         when you are done.
	 */
	@Override
	public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
		// TODO give each thread a subtree in the branch selector
		// branchStrategy.addThreadWriter(p, mythreadid, threadcount,
		// versionGraph.getNode(rootBranchName).getData());
		return true;
	}

	private void addOperations(String opName, int numOps) {
		for (int i = 0; i < numOps; ++i) {
			operations.add(opName);
		}
	}

	public void setupTransactionDistribution(Properties p) throws WorkloadException {

		int numInserts = Integer.parseInt(p.getProperty(NUM_INSERTS_PROPERTY, NUM_INSERTS_PROPERTY_DEFAULT));
		if (numInserts > 0) {
			addOperations(NUM_INSERTS_PROPERTY, numInserts);
		}

		int numUpdates = Integer.parseInt(p.getProperty(NUM_UPDATES_PROPERTY, NUM_UPDATES_PROPERTY_DEFAULT));
		if (numUpdates > 0) {
			addOperations(NUM_UPDATES_PROPERTY, numUpdates);
		}

		int numDeletes = Integer.parseInt(p.getProperty(NUM_DELETES_PROPERTY, NUM_DELETES_PROPERTY_DEFAULT));
		if (numDeletes > 0) {
			addOperations(NUM_DELETES_PROPERTY, numDeletes);
		}

		int numScans = Integer.parseInt(p.getProperty(NUM_SCANS_PROPERTY, NUM_SCANS_PROPERTY_DEFAULT));
		if (numScans > 0) {
			addOperations(NUM_SCANS_PROPERTY, numScans);
		}

		int numBranches = Integer.parseInt(p.getProperty(NUM_BRANCHES_PROPERTY, NUM_BRANCHES_PROPERTY_DEFAULT));
		if (numBranches > 0) {
			addOperations(NUM_BRANCHES_PROPERTY, numBranches);
		}

		int numMerges = Integer.parseInt(p.getProperty(NUM_MERGES_PROPERTY, NUM_MERGES_PROPERTY_DEFAULT));
		if (numMerges > 0) {
			addOperations(NUM_MERGES_PROPERTY, numMerges);
		}

		int numReads = Integer.parseInt(p.getProperty(NUM_READS_PROPERTY, NUM_READS_PROPERTY_DEFAULT));
		if (numReads > 0) {
			addOperations(NUM_READS_PROPERTY, numReads);
		}

		int numQuery3 = Integer.parseInt(p.getProperty(NUM_QUERY_3_PROPERTY, NUM_QUERY_3_PROPERTY_DEFAULT));
		if (numQuery3 > 0) {
			addOperations(NUM_QUERY_3_PROPERTY, numQuery3);
		}

		int numQuery4 = Integer.parseInt(p.getProperty(NUM_QUERY_4_PROPERTY, NUM_QUERY_4_PROPERTY_DEFAULT));
		if (numQuery4 > 0) {
			addOperations(NUM_QUERY_4_PROPERTY, numQuery4);
		}

		int numDiff = Integer.parseInt(p.getProperty(NUM_DIFF_PROPERTY, NUM_DIFF_PROPERTY_DEFAULT));
		if (numDiff > 0) {
			addOperations(NUM_DIFF_PROPERTY, numDiff);
		}

		int numCheckouts = Integer.parseInt(p.getProperty(NUM_CHECKOUTS_PROPERTY, NUM_CHECKOUTS_PROPERTY_DEFAULT));
		if (numCheckouts > 0) {
			addOperations(NUM_CHECKOUTS_PROPERTY, numCheckouts);
		}
	}

	@Override
	public boolean doTransaction(VersionDB db, Object threadstate) {
		if (!operationsIt.hasNext()) {
			return false;
		}

		String op = operationsIt.next();

		TransactionExecutor te = new TransactionExecutor(db, table, op);

		if (op.compareTo(NUM_INSERTS_PROPERTY) == 0) {
			doTransactionInsert(te);
		} else if (op.compareTo(NUM_UPDATES_PROPERTY) == 0) {
			doTransactionUpdate(te);
		} else if (op.compareTo(NUM_DELETES_PROPERTY) == 0) {
			doTransactionDelete(te);
		} else if (op.compareTo(NUM_SCANS_PROPERTY) == 0) {
			doTransactionScan(te);
		} else if (op.compareTo(NUM_READS_PROPERTY) == 0) {
			doTransactionRead(te);
		} else if (op.compareTo(NUM_BRANCHES_PROPERTY) == 0) {
			doTransactionBranch(te);
		} else if (op.compareTo(NUM_MERGES_PROPERTY) == 0) {
			doTransactionMerge(te);
		} else if (op.compareTo(NUM_QUERY_3_PROPERTY) == 0) {
			doTransactionQUERY_3(te);
		} else if (op.compareTo(NUM_QUERY_4_PROPERTY) == 0) {
			doTransactionQUERY_4(te);
		} else if (op.compareTo(NUM_DIFF_PROPERTY) == 0) {
			doTransactionDIFF(te);
		} else if (op.compareTo(NUM_CHECKOUTS_PROPERTY) == 0) {
			doTransactionCheckout(te);
		} else {
			throw new RuntimeException("invalid operation!");
		}
		logger.debug("Executed Transaction: " + te);

		return true;
	}

	private void doTransactionCheckout(TransactionExecutor te) {

		Transaction t = new Transaction("CHECKOUT") {

			@Override
			public void execute(VersionDB db, String tableName) {
				TransactionId tid = db.startTransaction();
				db.randomCheckout(tid, tableName);
				db.commitTransaction(tid);
			}
		};
		te.setTransaction(t);
		te.doTransaction();

	}

	private void doTransactionDIFF(TransactionExecutor te) {
		CompareOpData[] compareOpData = branchStrategy.getNextForCompare();
		for (CompareOpData compareOp : compareOpData) {
			final String[] branchNamesToScan = compareOp.branches;
			final String fromBranchName = branchNamesToScan[0];
			final String toBranchName = branchNamesToScan[1];

			String subOperationName = compareOp.subOperationName + " (" + fromBranchName + " --> " + toBranchName + ")";

			Transaction t = new Transaction(subOperationName, compareOp.branches) {

				@Override
				public void execute(VersionDB db, String tableName) {
					TransactionId tid = db.startTransaction();
					db.diff(tid, tableName, fromBranchName, toBranchName);
					db.commitTransaction(tid);
				}
			};

			te.setTransaction(t);
			te.doTransaction();
			Client.dropCaches();
		}
	}

	private void doTransactionQUERY_4(TransactionExecutor te) {
		final int fieldno = 0;
		final int alpha = 0;

		Transaction t = new Transaction() {

			@Override
			public void execute(VersionDB db, String tableName) {
				TransactionId tid = db.startTransaction();
				db.QUERY_4(tid, tableName, fieldno, alpha);
				db.commitTransaction(tid);
			}
		};
		te.setTransaction(t);
		te.doTransaction();
	}

	private void doTransactionQUERY_3(TransactionExecutor te) {
		CompareOpData[] compareOpData = branchStrategy.getNextForCompare();
		for (CompareOpData compareOp : compareOpData) {
			final String[] branchNamesToScan = compareOp.branches;
			final String branchName1 = branchNamesToScan[0];
			final String branchName2 = branchNamesToScan[1];

			String subOperationName = compareOp.subOperationName + " (" + branchName1 + " <--> " + branchName2 + ")";

			// This represents the per-branch selectivity of the join.
			// So, how many records per branch will be a part of the join.
			int numIOOpsForQuery = 1000000; // At most 1000 records in join

			int predField = (int) Math.ceil(Math.sqrt(numIOOpsForQuery));
			Predicate pred = new Predicate(td.getPrimaryKeyIndex(), Predicate.Op.LESS_THAN, new IntField(predField));

			Transaction t = new Transaction(subOperationName, compareOp.branches) {

				@Override
				public void execute(VersionDB db, String tableName) {
					TransactionId tid = db.startTransaction();
					db.QUERY_3(tid, tableName, branchName1, branchName2, pred);
					db.commitTransaction(tid);
				}
			};
			te.setTransaction(t);
			te.doTransaction();
			Client.dropCaches();
		}
	}

	public void doTransactionScan(TransactionExecutor te) {
		final ScanOpData[] scanOpData = branchStrategy.getNextForScan();
		for (ScanOpData scanOp : scanOpData) {
			Transaction t = new Transaction(scanOp.subOperationName, scanOp.branch) {

				@Override
				public void execute(VersionDB db, String tableName) {
					TransactionId tid = db.startTransaction();
					db.scan(tid, null, tableName, rcv, scanOp.branch);
					db.commitTransaction(tid);
				}
			};

			te.setTransaction(t);
			te.doTransaction();
			Client.dropCaches();
		}
	}

	private void doTransactionDelete(TransactionExecutor te) {
		DeleteOpData opData = branchStrategy.getNextForDelete();
		final String branchName = opData.branch;
		IntList nextTupleIds = tupleLoadStrategy.getNextForDelete(branchName);

		for (IntField nextTupleId : nextTupleIds) {
			tup.setPrimaryKey(nextTupleId);

			Transaction t = new Transaction(opData.subOperationName, branchName) {

				@Override
				public void execute(VersionDB db, String tableName) {
					TransactionId tid = db.startTransaction();
					db.delete(tid, tableName, branchName, tup);
					db.commitTransaction(tid);
				}
			};
			te.setTransaction(t);
			te.doTransaction();
		}
	}

	public void doTransactionUpdate(TransactionExecutor te) {
		UpdateOpData opData = branchStrategy.getNextForUpdate();
		final String branchName = opData.branch;
		IntList nextTupleIds = tupleLoadStrategy.getNextForUpdate(branchName);

		for (IntField nextTupleId : nextTupleIds) {
			tup.setPrimaryKey(nextTupleId);

			Transaction t = new Transaction(opData.subOperationName, branchName) {

				@Override
				public void execute(VersionDB db, String tableName) {
					TransactionId tid = db.startTransaction();
					db.update(tid, tableName, branchName, tup);
					db.commitTransaction(tid);
				}
			};
			te.setTransaction(t);
			te.doTransaction();
		}

	}

	public void doTransactionRead(TransactionExecutor te) {
		ReadOpData opData = branchStrategy.getNextForRead();
		final String branchName = opData.branch;
		IntList nextTupleIds = tupleLoadStrategy.getNextForRead(branchName);

		for (IntField nextTupleId : nextTupleIds) {
			final IntField primaryKeyField = nextTupleId;
			Transaction t = new Transaction(opData.subOperationName, branchName) {

				@Override
				public void execute(VersionDB db, String tableName) {
					TransactionId tid = db.startTransaction();
					db.read(tid, tableName, branchName, primaryKeyField);
					db.commitTransaction(tid);
				}
			};
			te.setTransaction(t);
			te.doTransaction();
		}
	}

	public void doTransactionInsert(TransactionExecutor te) {
		InsertOpData opData = branchStrategy.getNextForInsert();
		final String branchName = opData.branch;
		IntList nextTupleIds = tupleLoadStrategy.getNextForInsert(branchName);

		for (IntField nextTupleId : nextTupleIds) {
			Field primaryKeyField = nextTupleId;
			tup.setPrimaryKey(primaryKeyField);

			Transaction t = new Transaction(opData.subOperationName, branchName) {

				@Override
				public void execute(VersionDB db, String tableName) {
					TransactionId tid = db.startTransaction();
					db.insert(tid, tableName, branchName, tup);
					db.commitTransaction(tid);
				}
			};
			te.setTransaction(t);
			te.doTransaction();
		}
	}

	private void doTransactionBranch(TransactionExecutor te) {
		BranchOpData branchOpData = branchStrategy.getNextForBranch();
		final String parentBranchName = branchOpData.parent;
		final String childBranchName = branchOpData.child;

		// update load strategy so it knows about the new branch
		tupleLoadStrategy.addBranch(childBranchName, parentBranchName);

		Transaction t = new Transaction(branchOpData.subOperationName, parentBranchName) {

			@Override
			public void execute(VersionDB db, String tableName) {
				TransactionId tid = db.startTransaction();
				db.branch(tid, tableName, parentBranchName, childBranchName);
				db.commitTransaction(tid);
			}
		};
		te.setTransaction(t);
		te.doTransaction();
	}

	private void doTransactionMerge(TransactionExecutor te) {
		MergeOpData mergeOpData = branchStrategy.getNextForMerge();
		final String[] parentBranchNames = mergeOpData.parents;

		Transaction t = new Transaction(mergeOpData.subOperationName, parentBranchNames) {

			@Override
			public void execute(VersionDB db, String tableName) {
				TransactionId tid = db.startTransaction();
				db.merge(tid, tableName, parentBranchNames);
				db.commitTransaction(tid);
			}
		};
		te.setTransaction(t);
		te.doTransaction();
	}

	@Override
	public void cleanup() {
		logger.info("Cleaning up workload!");
	}
}
