package simpledb.versioned.benchmark;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableList;

import simpledb.Catalog;
import simpledb.Constants;
import simpledb.Database;
import simpledb.DbException;
import simpledb.DbFile;
import simpledb.DbFileIterator;
import simpledb.DbIterator;
import simpledb.Field;
import simpledb.HeapFile;
import simpledb.IntField;
import simpledb.Predicate;
import simpledb.Predicate.Op;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.versioned.BasicVersionSearchIterator;
import simpledb.versioned.BranchId;
import simpledb.versioned.CommitId;
import simpledb.versioned.VersionPredicate;
import simpledb.versioned.VersionSearchIterator;
import simpledb.versioned.VersionedBufferPool;
import simpledb.versioned.VersionedDbFile;
import simpledb.versioned.VersionedDbFile.MergeSpec;
import simpledb.versioned.benchmark.ycsb.DBException;
import simpledb.versioned.benchmark.ycsb.VersionDB;
import simpledb.versioned.heapfile.git.GitVersionedHeapFile;
import simpledb.versioned.heapfile.git2.GitV2VersionedHeapFile;
import simpledb.versioned.heapfile.hybrid.HybridDbFile;
import simpledb.versioned.heapfile.tuplefirst.MonolithicVersionedHeapFile;
//import simpledb.versioned.heapfile.git.GitVersionedHeapFile;
//import simpledb.versioned.heapfile.hybrid.HybridDbFile;
//import simpledb.versioned.heapfile.tuplefirst.MonolithicVersionedHeapFile;
import simpledb.versioned.heapfile.versionfirst.VersionedHeapFile;
import simpledb.versioned.utility.HeapFileGenerator;

/**
 * TODO: change all try/catch to use lambdas to reduce repeated code
 * 
 * Everything in the file with respect to commits is temporary, will abstract it
 * away properly.
 */
public class SimpleDBWrapper extends VersionDB {

	static final Logger logger = Logger.getLogger(SimpleDBWrapper.class);

	static final String VF_TYPE = "VF";
	static final String TF_TYPE = "TF";
	static final String HYBRID_TYPE = "HYBRID";
	static final String GIT_TYPE = "GIT";
	static final String GIT2_TYPE = "GIT2";
	static final List<String> TYPES = ImmutableList.of(VF_TYPE, TF_TYPE, HYBRID_TYPE, GIT_TYPE, GIT2_TYPE);

	public static class VersionedDbFileUtility {
		public static VersionSearchIterator makeVersionSearchIterator(VersionedDbFile vdbf, TransactionId tid,
				VersionPredicate vpred) {
			return new BasicVersionSearchIterator(vdbf, tid, vpred, false);
		}

		public static VersionedDbFile makeVersionFirstFile(File versionFile, TupleDesc td) {
			return new VersionedHeapFile(versionFile, td);
		}

		public static VersionedDbFile makeTupleFirstFile(File versionFile, TupleDesc td) {
			return new MonolithicVersionedHeapFile(versionFile, td);
		}

		public static VersionedDbFile makeHybridFile(File versionFile, TupleDesc td) {
			return new HybridDbFile(versionFile, td);
		}

		public static VersionedDbFile makeGitFile(File versionFile, TupleDesc td) {
			return new GitVersionedHeapFile(versionFile, td);
		}

		public static VersionedDbFile makeGit2File(File versionFile, TupleDesc td) {
			return new GitV2VersionedHeapFile(versionFile, td);
		}
	}

	public static class SimpleDbTupleSrc implements TupleSrc {

		private final DbFile file;
		private DbFileIterator iterator;
		private boolean open;

		public SimpleDbTupleSrc(DbFile file) {
			this.file = file;
			open = false;
		}

		@Override
		public void open(TransactionId tid) throws DbException, TransactionAbortedException {
			iterator = file.iterator(tid);
			iterator.open();
			open = true;

		}

		@Override
		public void close() {
			if (open) {
				iterator.close();
				iterator = null;
				open = false;
			}

		}

		@Override
		public boolean hasNext() {
			if (!open) {
				throw new IllegalStateException("Not open!");
			}
			try {
				return iterator.hasNext();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		}

		@Override
		public Tuple next() {
			if (!open) {
				throw new IllegalStateException("Not open!");
			}
			try {
				return iterator.next();
			} catch (NoSuchElementException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
		}

		@Override
		public void forEachRemaining(Consumer<? super Tuple> action) {
			// TODO Auto-generated method stub
		}

		@Override
		public void rewind() {
			if (!open) {
				throw new IllegalStateException("Not open!");
			}
			try {
				iterator.rewind();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private final Map<String, List<CommitId>> commits;
	private Random checkoutSelector;

	public SimpleDBWrapper() {
		commits = new HashMap<String, List<CommitId>>();
		try {
			checkoutSelector = new Random(
					Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.CHECKOUT_SELECTOR_SEED_MUL);
		} catch (Exception ex) {
			throw new RuntimeException();
		}
	}

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	@Override
	public void init() throws DBException {
		// TODO: fix this at some point
		// Database.reset();
	}

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException {
		// do nothing
	}

	@Override
	public void deleteTable(String tableName) {
		Integer tableId = getTableId(tableName);
		deleteTableData(tableId);
		Database.getCatalog().removeTable(tableId);
	}

	@Override
	public void clearDatabase() {
		Iterator<Integer> tableidIterator = Database.getCatalog().tableIdIterator();
		logger.info("Cleaning up files!");
		while (tableidIterator.hasNext()) {
			Integer tableId = tableidIterator.next();
			deleteTableData(tableId);
		}

		Database.getCatalog().clear();
	}

	private void deleteTableData(int tableId) {
		DbFile dbf = Database.getCatalog().getDatabaseFile(tableId);

		try {
			// close the file
			dbf.close();

			// delete all files
			List<File> files = dbf.getFiles();
			for (File file : files) {
				logger.info("Deleting: " + file);
				if (file.exists()) {
					FileUtils.forceDelete(file);
				}
				// boolean deleted = file.delete();
				// if (!deleted) {
				// logger.warn("Failed to delete: " + file);
				// }
			}
		} catch (IOException e) {
			logger.warn("Failed to delete: ", e);
		}

	}

	@Override
	public void force(String tableName) throws IOException {
		Database.getCatalog().getDatabaseFile(getTableId(tableName)).force();
	}

	private BranchId getBranchId(TransactionId tid, String tableName, String branchName) {
		VersionedDbFile vdf = (VersionedDbFile) Database.getCatalog().getDatabaseFile(getTableId(tableName));
		BranchId bid = vdf.getBranchIdByName(branchName);
		return bid;
	}

	private CommitId getCreationCommitId(TransactionId tid, String tableName, String branchName) {
		VersionedDbFile vdf = (VersionedDbFile) Database.getCatalog().getDatabaseFile(getTableId(tableName));
		BranchId bid = vdf.getBranchIdByName(branchName);
		return vdf.getCreationCommitId(bid);
	}

	private int getTableId(String tableName) {
		Catalog catalog = Database.getCatalog();
		int tableId = catalog.getTableId(tableName);
		return tableId;
	}

	private int opCount = -1;

	@Override
	public int getLastOpCount() {
		return opCount;
	}

	private void exceuteOp(DbIterator op) {
		opCount = 0;
		try {
			op.open();
			while (op.hasNext()) {
				op.next();
				opCount++;
			}
		} catch (DbException | TransactionAbortedException e) {
			throw new RuntimeException(e);
		} finally {
			op.close();
		}
	}

	@Override
	public void flushAllPages() throws IOException {
		Database.getBufferPool().flushAllPages();
	}

	@Override
	public void setNumBufferPoolPages(int numPages) {
		Database.resetBufferPool(numPages);

	}

	@Override
	public List<String> getVersionDBFileTypes() {
		return TYPES;
	}

	@Override
	public void setPageSize(int pageSize) {
		VersionedBufferPool.setPageSize(pageSize);
	}

	@Override
	public void merge(TransactionId tid, String tableName, String[] parents) {
		opCount = 1;
		if (parents.length != 2) {
			throw new IllegalArgumentException("Wrong number of parents for merge!");
		}
		BranchId[] bids = new BranchId[parents.length];
		for (int i = 0; i < parents.length; i++) {
			String branchName = parents[i];
			bids[i] = getBranchId(tid, tableName, branchName);
		}
		BranchId parent1 = bids[0];
		BranchId parent2 = bids[1];
		try {
			CommitId mergeCommit = Database.getBufferPool().merge(tid, tableName, parent1, parent2);
			commits.get(tableName).add(mergeCommit);
		} catch (DbException | TransactionAbortedException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void branch(TransactionId tid, String tableName, String parent, String childBranchName) {
		opCount = 1;
		BranchId parentBranchId = getBranchId(tid, tableName, parent);
		try {
			CommitId creationCommitId = Database.getBufferPool().branch(tid, tableName, parentBranchId,
					childBranchName);
			commits.get(tableName).add(creationCommitId);
		} catch (DbException | TransactionAbortedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void commit(TransactionId tid, String tableName, String branchName) {
		opCount = 1;
		BranchId bid = getBranchId(tid, tableName, branchName);
		try {
			CommitId newCommitId = Database.getBufferPool().commit(tid, tableName, bid);
			if (newCommitId == null) {
				throw new RuntimeException();
			}
			commits.get(tableName).add(newCommitId);
		} catch (IOException | DbException | TransactionAbortedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void randomCheckout(TransactionId tid, String tableName) {
		opCount = 1;
		List<CommitId> commitForTable = commits.get(tableName);
		CommitId selectedCommit = commitForTable.get(checkoutSelector.nextInt(commitForTable.size()));
		VersionedDbFile versionedDbFile = ((VersionedDbFile) Database.getCatalog()
				.getDatabaseFile(getTableId(tableName)));
		try {
			versionedDbFile.checkout(tid, selectedCommit);
		} catch (DbException | TransactionAbortedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long getSize(String tableName) {
		return Database.getCatalog().getDatabaseFile(getTableId(tableName)).getSize();
	}

	@Override
	public void prepareForWorkload(String tableName) {
		((VersionedDbFile) Database.getCatalog().getDatabaseFile(getTableId(tableName))).repack();
	}

	@Override
	public void close(String tableName) throws IOException {
		Database.getCatalog().getDatabaseFile(getTableId(tableName)).close();
	}

	@Override
	public String getVersionMetaDataStringRep(String tableName) {
		return ((VersionedDbFile) Database.getCatalog().getDatabaseFile(getTableId(tableName)))
				.getVersionMetaDataStringRep();
	}

	@Override
	public List<String> getBranches(TransactionId tid, String tableName) {
		return ((VersionedDbFile) Database.getCatalog().getDatabaseFile(getTableId(tableName))).getBranches(tid)
				.stream().map((Function<? super BranchId, String>) (bid) -> {
					return bid.getName();
				}).collect(Collectors.toList());
	}

	@Override
	public void createVersionedTable(TransactionId tid, String tableName, TupleDesc td, String type,
			MergeProc mergeProc) {
		File versionFile = new File(Database.getBaseDir(), type + UUID.randomUUID());
		VersionedDbFile vdf = null;
		if (type.equals(VF_TYPE)) {
			vdf = VersionedDbFileUtility.makeVersionFirstFile(versionFile, td);
		} else if (type.equals(TF_TYPE)) {
			vdf = VersionedDbFileUtility.makeTupleFirstFile(versionFile, td);
		} else if (type.equals(HYBRID_TYPE)) {
			vdf = VersionedDbFileUtility.makeHybridFile(versionFile, td);
		} else if (type.equals(GIT_TYPE)) {
			vdf = VersionedDbFileUtility.makeGitFile(versionFile, td);
		} else if (type.equals(GIT2_TYPE)) {
			versionFile.mkdir();
			vdf = VersionedDbFileUtility.makeGit2File(versionFile, td);
		} else {
			throw new IllegalArgumentException("Versioned Heap File Type: " + type + " not supported!");
		}

		Database.getCatalog().addTable(vdf, tableName);
		vdf.setup(tid);

		MergeSpec mergeSpec = null;

		switch (mergeProc) {
		case THREE_WAY:
			mergeSpec = MergeSpec.THREE_WAY;
			break;
		case TWO_WAY:
			mergeSpec = MergeSpec.TWO_WAY;
			break;
		}

		vdf.setMergeSpec(mergeSpec);

		// should only have master here
		List<CommitId> initialCommits = new ArrayList<CommitId>();
		int count = 0;
		for (BranchId bid : vdf.getBranches(tid)) {
			if (count > 1) {
				throw new IllegalStateException("Too many branches from load, expected 1. Not stateless.");
			}
			count++;
			CommitId creationCommitId = getCreationCommitId(tid, tableName, bid.getName());
			if (creationCommitId != null) {
				initialCommits.add(creationCommitId);
			}
		}
		commits.put(tableName, initialCommits);
	}

	@Override
	public TransactionId startTransaction() {
		return new TransactionId();
	}

	@Override
	public TupleSrc getNewTupleSrc(TupleDesc td, int numTuples) {
		try {
			DbFile file = HeapFileGenerator.createRandomHeapFile(td.numFields(), numTuples, Database.PAGE_SIZE);
			return new SimpleDbTupleSrc(file);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TupleSrc getTupleSrcFromFile(String fileName, TupleDesc td) {
		File tupleSrcFile = new File(Database.getBaseDir(), fileName);
		DbFile file = new HeapFile(tupleSrcFile, td);
		Database.getCatalog().addTable(file);
		return new SimpleDbTupleSrc(file);
	}

	@Override
	public void commitTransaction(TransactionId tid) {
		try {
			Database.getBufferPool().transactionComplete(tid);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void abortTransaction(TransactionId tid) {
		try {
			Database.getBufferPool().transactionComplete(tid, false);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void scan(TransactionId tid, Integer[] fields, String tableName, boolean reportContainingVersions,
			String... branchNames) {
		BranchId[] bids = new BranchId[branchNames.length];
		for (int i = 0; i < branchNames.length; i++) {
			String branchName = branchNames[i];
			bids[i] = getBranchId(tid, tableName, branchName);
			if (bids[i] == null) {
				throw new IllegalArgumentException("Branch does not exist: " + branchName);
			}
		}
		DbIterator scanOp;
		try {
			scanOp = Database.getBufferPool().scan(tid, tableName, reportContainingVersions, bids);
			exceuteOp(scanOp);
		} catch (DbException | TransactionAbortedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void insert(TransactionId tid, String tableName, String branchName, Tuple tup) {
		try {
			BranchId bid = getBranchId(tid, tableName, branchName);
			DbIterator insertOp = Database.getBufferPool().insert(tid, tableName, bid, tup);
			exceuteOp(insertOp);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void update(TransactionId tid, String tableName, String branchName, Tuple tup) {
		try {
			BranchId bid = getBranchId(tid, tableName, branchName);
			DbIterator updateOp = Database.getBufferPool().update(tid, tableName, bid, tup);
			exceuteOp(updateOp);
		} catch (DbException | TransactionAbortedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void read(TransactionId tid, String tableName, String branchName, Field primaryKey) {
		try {
			BranchId bid = getBranchId(tid, tableName, branchName);
			DbIterator readOp = Database.getBufferPool().read(tid, tableName, bid, primaryKey);
			exceuteOp(readOp);
		} catch (DbException | TransactionAbortedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(TransactionId tid, String tableName, String branchName, Tuple tup) {
		try {
			BranchId bid = getBranchId(tid, tableName, branchName);
			DbIterator deleteOp = Database.getBufferPool().delete(tid, tableName, bid, tup);
			exceuteOp(deleteOp);
		} catch (DbException | TransactionAbortedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void QUERY_3(TransactionId tid, String tableName, String branchName1, String branchName2, Predicate pred) {
		try {
			BranchId bid1 = getBranchId(tid, tableName, branchName1);
			BranchId bid2 = getBranchId(tid, tableName, branchName2);
			DbIterator q3Op = Database.getBufferPool().QUERY_3(tid, tableName, bid1, bid2, pred);
			exceuteOp(q3Op);
		} catch (DbException | TransactionAbortedException | NoSuchElementException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void QUERY_4(TransactionId tid, String tableName, int fieldno, int alpha) {
		try {
			Predicate pred = new Predicate(fieldno, Op.GREATER_THAN_OR_EQ, new IntField(alpha));
			DbIterator q4Op = Database.getBufferPool().QUERY_4(tid, tableName, pred);
			exceuteOp(q4Op);
		} catch (DbException | TransactionAbortedException | NoSuchElementException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void diff(TransactionId tid, String tableName, String fromBranchName, String toBranchName) {
		try {
			BranchId from = getBranchId(tid, tableName, fromBranchName);
			BranchId to = getBranchId(tid, tableName, toBranchName);
			DbIterator diffOp = Database.getBufferPool().diff(tid, tableName, from, to);
			exceuteOp(diffOp);
		} catch (DbException | TransactionAbortedException | NoSuchElementException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * QUERIES THAT ARE NOT SUPPORTED!
	 */

	@Override
	public TupleSrc getTableScanner(TransactionId tid, String... branchNames) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createVersionedTableFromFile(TransactionId tid, String filename, String tableName, TupleDesc desc,
			String type) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void copyVersionedTable(TransactionId tid, String tableName, String newTableName) {
		throw new UnsupportedOperationException();
	}

}
