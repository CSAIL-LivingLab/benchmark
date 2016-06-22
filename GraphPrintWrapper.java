package simpledb.versioned.benchmark;

import java.io.IOException;
import java.util.List;

import simpledb.Field;
import simpledb.Predicate;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.versioned.benchmark.ycsb.VersionDB;
import simpledb.versioned.utility.PrettyVersionGraph;

public class GraphPrintWrapper extends VersionDB {

	private final VersionDB wrappedFile;

	private final PrettyVersionGraph vGraph;

	@Override
	public int getLastOpCount() {
		return wrappedFile.getLastOpCount();
	}

	public GraphPrintWrapper(VersionDB wrappedFile) {
		this.wrappedFile = wrappedFile;
		vGraph = new PrettyVersionGraph();
		setMeasurements(wrappedFile.getMeasurements());
	}

	@Override
	public void createVersionedTable(TransactionId tid, String tableName, TupleDesc desc, String type,
			MergeProc mergeProc) {
		wrappedFile.createVersionedTable(tid, tableName, desc, type, mergeProc);
	}

	@Override
	public void merge(TransactionId tid, String tableName, String[] parents) {
		String parent1 = parents[0];
		String parent2 = parents[1];
		if (!vGraph.containsVertex(parent1)) {
			vGraph.addVertex(parent1);
		}
		if (!vGraph.containsVertex(parent2)) {
			vGraph.addVertex(parent2);
		}
		wrappedFile.merge(tid, tableName, parents);
		// TODO: fix this at some point
		vGraph.handleMerge(parent1, parent2);
	}

	@Override
	public void branch(TransactionId tid, String tableName, String parentBranchName, String newBranchName) {
		wrappedFile.branch(tid, tableName, parentBranchName, newBranchName);
		if (!vGraph.containsVertex(parentBranchName)) {
			vGraph.addVertex(parentBranchName);
		}
		vGraph.handleBranch(parentBranchName, newBranchName);
	}

	public void printGraph() {
		vGraph.printGraph();
	}

	public void waitForGraph() {
		vGraph.waitForGraph();
	}

	@Override
	public List<String> getVersionDBFileTypes() {
		return wrappedFile.getVersionDBFileTypes();
	}

	@Override
	public TransactionId startTransaction() {
		return wrappedFile.startTransaction();
	}

	@Override
	public void setNumBufferPoolPages(int numPages) {
		wrappedFile.setNumBufferPoolPages(numPages);
	}

	@Override
	public void commitTransaction(TransactionId tid) {
		wrappedFile.commitTransaction(tid);
	}

	@Override
	public void abortTransaction(TransactionId tid) {
		wrappedFile.abortTransaction(tid);
	}

	@Override
	public void scan(TransactionId tid, Integer[] fields, String tableName, boolean reportContainingVersions,
			String... branchNames) {
		wrappedFile.scan(tid, fields, tableName, reportContainingVersions, branchNames);
	}

	@Override
	public TupleSrc getTableScanner(TransactionId tid, String... branchNames) {
		return wrappedFile.getTableScanner(tid, branchNames);
	}

	@Override
	public void insert(TransactionId tid, String tableName, String branchName, Tuple tup) {
		wrappedFile.insert(tid, tableName, branchName, tup);
	}

	@Override
	public void update(TransactionId tid, String tableName, String branchName, Tuple tup) {
		wrappedFile.update(tid, tableName, branchName, tup);
	}

	@Override
	public void read(TransactionId tid, String tableName, String branchName, Field primaryKey) {
		wrappedFile.read(tid, tableName, branchName, primaryKey);
	}

	@Override
	public void delete(TransactionId tid, String tableName, String branchName, Tuple tup) {
		wrappedFile.delete(tid, tableName, branchName, tup);
	}

	@Override
	public TupleSrc getNewTupleSrc(TupleDesc td, int numTuples) {
		return wrappedFile.getNewTupleSrc(td, numTuples);
	}

	@Override
	public TupleSrc getTupleSrcFromFile(String fileName, TupleDesc td) {
		return wrappedFile.getTupleSrcFromFile(fileName, td);
	}

	@Override
	public void createVersionedTableFromFile(TransactionId tid, String filename, String tableName, TupleDesc desc,
			String type) {
		wrappedFile.createVersionedTableFromFile(tid, filename, tableName, desc, type);
	}

	@Override
	public void copyVersionedTable(TransactionId tid, String tableName, String newTableName) {
		wrappedFile.copyVersionedTable(tid, tableName, newTableName);
	}

	@Override
	public void setPageSize(int pageSize) {
		wrappedFile.setPageSize(pageSize);
	}

	@Override
	public void flushAllPages() throws IOException {
		wrappedFile.flushAllPages();
	}

	@Override
	public long getSize(String tableName) {
		return wrappedFile.getSize(tableName);
	}

	@Override
	public void clearDatabase() {
		wrappedFile.clearDatabase();
	}

	@Override
	public void deleteTable(String tableName) {
		wrappedFile.deleteTable(tableName);
	}

	@Override
	public void QUERY_3(TransactionId tid, String tableName, String branchName1, String branchName2, Predicate pred) {
		wrappedFile.QUERY_3(tid, tableName, branchName1, branchName2, pred);
	}

	@Override
	public void QUERY_4(TransactionId tid, String tableName, int fieldno, int alpha) {
		wrappedFile.QUERY_4(tid, tableName, fieldno, alpha);
	}

	@Override
	public void diff(TransactionId tid, String tableName, String fromBranchName, String toBranchName) {
		wrappedFile.diff(tid, tableName, fromBranchName, toBranchName);
	}

	@Override
	public String getVersionMetaDataStringRep(String tableName) {
		return wrappedFile.getVersionMetaDataStringRep(tableName);
	}

	@Override
	public void commit(TransactionId tid, String tableName, String branchName) {
		wrappedFile.commit(tid, tableName, branchName);
	}

	@Override
	public void randomCheckout(TransactionId tid, String tableName) {
		wrappedFile.randomCheckout(tid, tableName);
	}

	@Override
	public List<String> getBranches(TransactionId tid, String tableName) {
		return wrappedFile.getBranches(tid, tableName);
	}

	@Override
	public void close(String tableName) throws IOException {
		wrappedFile.close(tableName);

	}

	@Override
	public void force(String tableName) throws IOException {
		wrappedFile.force(tableName);
	}

	@Override
	public void prepareForWorkload(String tableName) {
		wrappedFile.prepareForWorkload(tableName);

	}

}
