package simpledb.versioned.benchmark;

import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import simpledb.Constants;
import simpledb.DbException;
import simpledb.IntField;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.BranchOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.InsertOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.MergeOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.TreeModOpData;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy.UpdateOpData;
import simpledb.versioned.benchmark.loaddistribution.LoadDistribution;
import simpledb.versioned.benchmark.tupleloadstrategy.TupleLoadStrategy;
import simpledb.versioned.benchmark.ycsb.Scenario.VersionedTableEntry;
import simpledb.versioned.benchmark.ycsb.VersionDB;
import simpledb.versioned.benchmark.ycsb.VersionDB.Transaction;
import simpledb.versioned.benchmark.ycsb.VersionDB.TransactionExecutor;
import simpledb.versioned.benchmark.ycsb.VersionDB.TupleSrc;
import simpledb.versioned.benchmark.ycsb.generator.DiscreteGenerator;

/**
 * Provides at least semantics, will grow the structure to at least the target
 * size.
 * 
 * @author David
 * 
 */
public class VersionGraphBuilder {
	static final Logger logger = Logger.getLogger(VersionGraphBuilder.class);

	static final String MERGE_OP_NAME = "MERGE";
	static final String BRANCH_OP_NAME = "BRANCH";
	static final String UPDATE_OP_NAME = "UPDATE";
	static final String NO_UPDATE_OP_NAME = "NO UPDATE";
	final VersionDB db;
	final TupleDesc td;
	final Type primaryKeyType;
	final int primaryKeyIndex;
	final VersionedTableEntry table;
	final TupleSrc tupleSrc;
	final TransactionId systemTransaction;
	final String startBranchRoot;
	final LoadDistribution distribution;
	final BranchStrategy branchStrategy;
	final TupleLoadStrategy tupleLoadStrategy;
	final DiscreteGenerator treeOperationChooser;
	final DiscreteGenerator updateOperationChooser;
	final double branchSpacingFactor;
	final boolean removeBranchCap;
	final int numInsertsUpdatesIntoBranchBeforeCommit;
	final Random insertUpdate, treeMod;

	public VersionGraphBuilder(VersionDB db, VersionedTableEntry table, TransactionId systemTransaction, TupleDesc td,
			String startBranchRoot, BranchStrategy branchStrategy, LoadDistribution distribution,
			TupleLoadStrategy tupleLoadStrategy, TupleSrc tupleSrc, double initialBranchProportion,
			double initialMergeProportion, double initialUpdateProbability, double branchSpacingFactor,
			boolean removeBranchCap, int numInsertsUpdatesIntoBranchBeforeCommit) {

		try {
			insertUpdate = new Random(
					Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.V_GRAPH_BUILD_SEED_MUL);
			treeMod = new Random(
					Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.V_GRAPH_BUILD_SEED_MUL);
		} catch (Exception ex) {
			throw new RuntimeException();
		}

		this.db = db;
		this.startBranchRoot = startBranchRoot;
		this.td = td;
		primaryKeyIndex = td.getPrimaryKeyIndex();
		primaryKeyType = td.getFieldType(primaryKeyIndex);
		this.systemTransaction = systemTransaction;
		this.table = table;
		this.branchStrategy = branchStrategy;
		this.distribution = distribution;
		this.tupleLoadStrategy = tupleLoadStrategy;
		this.tupleSrc = tupleSrc;
		this.branchSpacingFactor = branchSpacingFactor;
		this.removeBranchCap = removeBranchCap;
		this.numInsertsUpdatesIntoBranchBeforeCommit = numInsertsUpdatesIntoBranchBeforeCommit;

		treeOperationChooser = new DiscreteGenerator(treeMod);
		treeOperationChooser.addValue(initialBranchProportion, BRANCH_OP_NAME);
		treeOperationChooser.addValue(initialMergeProportion, MERGE_OP_NAME);

		updateOperationChooser = new DiscreteGenerator(insertUpdate);
		updateOperationChooser.addValue(initialUpdateProbability, UPDATE_OP_NAME);
		updateOperationChooser.addValue(1 - initialUpdateProbability, NO_UPDATE_OP_NAME);
	}

	public void build() throws DbException, TransactionAbortedException {
		// idea here is to insert X number of tuples according to how they would
		// be inserted by the branching strategy
		// while at the same time inserting enough into a given branch to get
		// the specified physical properties like
		// page clustering

		// start building
		tupleSrc.open(systemTransaction);

		// initialization
		int numTreeOpsRemaining = distribution.getTotalInitialNumNewBranches();

		distribution.addBranch(startBranchRoot);
		branchStrategy.init(startBranchRoot);
		tupleLoadStrategy.addBranch(startBranchRoot);
		doTargetBranchLoad(systemTransaction, startBranchRoot);
		logger.debug("initialized and loaded root branch: " + startBranchRoot);

		// generate the remaining ops
		while (numTreeOpsRemaining > 0) {
			// initialize the branch strategy so that all starting conditions
			// met
			// there may be a string of these
			TreeModOpData currentOp = branchStrategy.getNextForInit();
			if (currentOp == null) {
				String nextOp = treeOperationChooser.nextString();
				if (nextOp.equals(BRANCH_OP_NAME)) {
					currentOp = branchStrategy.getNextForBranch();
				} else if (nextOp.equals(MERGE_OP_NAME)) {
					currentOp = branchStrategy.getNextForMerge();
				} else {
					throw new IllegalStateException();
				}
			}
			numTreeOpsRemaining--;
			executeTreeModOp(systemTransaction, currentOp);
			if (logger.isDebugEnabled()) {
				logger.debug("Executing tree mod op! num remaining: " + numTreeOpsRemaining);
			}
		}
		if (!removeBranchCap) {
			logger.debug("Executing global load, remaing tuples to insert are: "
					+ distribution.getTotalNumTuplesRemaining());
			doGlobalTargetLoad(systemTransaction);
			logger.debug("Finished loading, loaded:" + distribution.getTotalNumInserted());
			tupleSrc.close();
		}

		List<String> branches = db.getBranches(systemTransaction, table.tableName);
		for (String branchName : branches) {
			db.commit(systemTransaction, table.tableName, branchName);
			// doCommit(systemTransaction, branchName);
		}
	}

	private void executeTreeModOp(TransactionId tid, TreeModOpData treeMod) {
		// do next tree mod operation
		if (treeMod instanceof BranchOpData) {
			BranchOpData branchOp = (BranchOpData) treeMod;

			logger.debug("Executing branch op: " + branchOp);

			// update tuple load strategy so that it knows about the new
			// branch
			distribution.addBranch(branchOp.child);
			tupleLoadStrategy.addBranch(branchOp.child, branchOp.parent);

			doBranch(tid, branchOp);

			// Only load following a genuine branch versus a merge
			doTargetBranchLoad(tid, branchOp.child);
		} else if (treeMod instanceof MergeOpData) {
			MergeOpData mergeOp = (MergeOpData) treeMod;

			logger.debug("Executing merge op: " + mergeOp);

			doMerge(tid, mergeOp);
		} else {
			throw new IllegalStateException();
		}
	}

	private void doInsert(TransactionId tid, String branchName) {
		IntList primaryKeyToInsert = tupleLoadStrategy.getNextForInsert(branchName);
		int numInserted = 0;
		for (IntField primaryKey : primaryKeyToInsert) {
			Tuple currentTuple = tupleSrc.next();
			currentTuple.setField(primaryKeyIndex, primaryKey);

			db.insert(tid, table.tableName, branchName, currentTuple);
			numInserted++;

			if (logger.isDebugEnabled()) {
				logger.debug("Inserted " + primaryKey + " into " + branchName);
			}
		}
		if (numInserted > 1) {
			logger.debug("Total inserted " + numInserted + " into " + branchName);
		}
		distribution.updateNumInsertedUpdated(branchName, numInserted);
	}

	private void doUpdate(TransactionId tid, String branchName) {
		IntList primaryKeyToUpdate = tupleLoadStrategy.getNextForUpdate(branchName);
		int numUpdated = 0;
		for (IntField primaryKey : primaryKeyToUpdate) {
			// get a new tuple, but change the primary key so it looks like a
			// tuple just being updated
			Tuple currentTuple = tupleSrc.next();
			currentTuple.setField(primaryKeyIndex, primaryKey);

			db.update(tid, table.tableName, branchName, currentTuple);
			numUpdated++;

			if (logger.isDebugEnabled()) {
				logger.debug("Updated " + primaryKey + " into " + branchName);
			}
		}
		if (numUpdated > 1) {
			logger.debug("Total updated: " + numUpdated + " into " + branchName);
		}

		// Count updates toward insert count to constraint global data set size
		distribution.updateNumInsertedUpdated(branchName, numUpdated);
	}

	private void doTargetBranchLoad(TransactionId tid, String targetBranchName) {
		logger.debug("doing target for: " + targetBranchName);
		int minNumToInsert = (int) Math.ceil(branchSpacingFactor * distribution.getRemainingToInsert(targetBranchName));
		// upon first inserting into a branch, ensure it reaches its
		// initial capacity
		int numInsertedSoFar = distribution.getNumInsertedUpdated(targetBranchName);
		while (numInsertedSoFar < minNumToInsert) {
			doInsertUpdateCycle(tid);
			numInsertedSoFar = distribution.getNumInsertedUpdated(targetBranchName);
		}
	}

	private void doGlobalTargetLoad(TransactionId tid) {
		while (distribution.getTotalNumTuplesRemaining() > 0) {
			doInsertUpdateCycle(tid);
		}
	}

	private void doInsertUpdateCycle(TransactionId tid) {
		String nextUpdate = updateOperationChooser.nextString();
		String branchInsertedUpdated = null;
		if (nextUpdate.equals(UPDATE_OP_NAME)) {
			branchInsertedUpdated = performUpdateCycle(tid);
		} else {
			branchInsertedUpdated = performInsertCycle(tid);
		}
		// commit points not exact, but this is simple to understand and the
		// point is to add in commits. If we want this to be more exact we need
		// to add this logic in doUpdate and doInsert
		if (shouldCommit(branchInsertedUpdated)) {
			if (logger.isDebugEnabled()) {
				logger.debug("Commiting: " + branchInsertedUpdated + " at num inserted/updated: "
						+ distribution.getNumInsertedUpdated(branchInsertedUpdated));
			}
			doCommit(tid, branchInsertedUpdated);
			distribution.resetNumTuplesInsertedUpdatedSinceLastCommit(branchInsertedUpdated);
		}
	}

	private boolean shouldCommit(String branchName) {
		return distribution
				.getNumTuplesInsertedUpdatedSinceLastCommit(branchName) >= numInsertsUpdatesIntoBranchBeforeCommit;
	}

	private String performUpdateCycle(TransactionId tid) {
		UpdateOpData updateOpData = branchStrategy.getNextForUpdate();
		String nextBranchToUpdate = updateOpData.branch;

		if (!removeBranchCap) {
			// need breaking counter in case branching strategy keeps returning
			// the same branch in an infinite loop (e.g. deep at the end)
			int count = 0;
			while (distribution.getRemainingToInsert(nextBranchToUpdate) <= 0
					&& count < 2 * distribution.getTotalInitialNumBranches()) {
				updateOpData = branchStrategy.getNextForUpdate();
				nextBranchToUpdate = updateOpData.branch;
				count++;
			}
		}

		if (distribution.getNumInsertedUpdated(nextBranchToUpdate) > 0) {
			doUpdate(tid, nextBranchToUpdate);
			return nextBranchToUpdate;
		} else {
			// If the chosen branch has no records to update,
			// just default to insert.
			return performInsertCycle(tid);
		}
	}

	private String performInsertCycle(TransactionId tid) {
		InsertOpData insertOpData = branchStrategy.getNextForInsert();
		String nextBranchToInsertInto = insertOpData.branch;
		if (!removeBranchCap) {
			// need breaking counter in case branching strategy keeps returning
			// the same branch in an infinite loop (e.g. deep at the end)
			int count = 0;
			while (distribution.getRemainingToInsert(nextBranchToInsertInto) <= 0
					&& count < 2 * distribution.getTotalInitialNumBranches()) {
				insertOpData = branchStrategy.getNextForInsert();
				nextBranchToInsertInto = insertOpData.branch;
				count++;
			}
		}
		doInsert(tid, nextBranchToInsertInto);
		return nextBranchToInsertInto;
	}

	private void doCommit(TransactionId tid, String branchName) {
		TransactionExecutor te = new TransactionExecutor(db, table, CoreVersionWorkload.NUM_COMMITS_PROPERTY);
		Transaction t = new Transaction(branchName) {

			@Override
			public void execute(VersionDB db, String tableName) {
				db.commit(tid, tableName, branchName);
			}
		};
		te.setTransaction(t);
		te.doTransaction();
	}

	private void doBranch(TransactionId tid, BranchOpData branchOp) {
		TransactionExecutor te = new TransactionExecutor(db, table, CoreVersionWorkload.NUM_BRANCHES_PROPERTY);
		Transaction t = new Transaction(branchOp.subOperationName, branchOp.parent) {

			@Override
			public void execute(VersionDB db, String tableName) {
				db.branch(tid, tableName, branchOp.parent, branchOp.child);
			}
		};
		te.setTransaction(t);
		te.doTransaction();
	}

	private void doMerge(TransactionId tid, MergeOpData mergeOp) {
		TransactionExecutor te = new TransactionExecutor(db, table, CoreVersionWorkload.NUM_MERGES_PROPERTY);
		Transaction t = new Transaction(mergeOp.subOperationName, mergeOp.parents) {

			@Override
			public void execute(VersionDB db, String tableName) {
				db.merge(tid, tableName, mergeOp.parents);
			}
		};
		te.setTransaction(t);
		te.doTransaction();
	}
}
