package simpledb.versioned.benchmark.branchstrategy;

import java.util.List;
import java.util.Random;

//not thread safe
public abstract class BranchStrategy {
	static final String EDGE_SYMBOL = "->";
	static final int MAX_NUM_BRANCHES_TO_MERGE = 2;
	static final String BRANCH_NAME_BASE = "branch";
	static final int MAX_BRANCH_NAME_LENGTH = BRANCH_NAME_BASE.length() + 5;
	static final String NO_SUB_OPERATION_NAME = "NONE";

	public static class OpData {
		public String subOperationName;

		public OpData() {
			subOperationName = NO_SUB_OPERATION_NAME;
		}

		public void setSubOperationName(String subOperationName) {
			this.subOperationName = subOperationName;
		}
	}

	public static class BasicOpData extends OpData {
		public final String branch;

		public BasicOpData(String branch) {
			this.branch = branch;
		}

	}

	public static abstract class TreeModOpData extends OpData {

	}

	// TODO: add methods to each of these instead of accessing attributes
	// directly
	public static class BranchOpData extends TreeModOpData {
		public final String parent;
		public final String child;

		public BranchOpData(String parent, String child) {
			this.parent = parent;
			this.child = child;
		}

		public String getChild() {
			return child;
		}

		@Override
		public String toString() {
			return "BRANCH:" + parent + EDGE_SYMBOL + child;
		}
	}

	public static class MergeOpData extends TreeModOpData {

		public final String[] parents;

		public MergeOpData(String[] parents) {
			this.parents = parents;
		}

		@Override
		public String toString() {
			return "MERGE: (" + parents[0] + "," + parents[1] + ") ";
		}

	}

	public static class ScanOpData extends OpData {
		public final String branch;

		public ScanOpData(String branch) {
			this.branch = branch;
		}

	}

	public static class DeleteOpData extends BasicOpData {

		public DeleteOpData(String branch) {
			super(branch);
		}

	}

	public static class InsertOpData extends BasicOpData {

		public InsertOpData(String branch) {
			super(branch);
		}

	}

	public static class UpdateOpData extends BasicOpData {

		public UpdateOpData(String branch) {
			super(branch);
		}

	}

	public static class ReadOpData extends BasicOpData {

		public ReadOpData(String branch) {
			super(branch);
		}
	}

	public static class CompareOpData extends OpData {
		public final String[] branches;

		public CompareOpData(String[] branches) {
			this.branches = branches;
		}
	}

	public static interface BranchSelector {
		String[] selectBranches(List<String> branchPool);
	}

	public static class UniformBranchSelector implements BranchSelector {
		private final Random rand;

		public UniformBranchSelector(Random rand) {
			this.rand = rand;
		}

		@Override
		public String[] selectBranches(List<String> branchPool) {
			int numBranchesInPool = branchPool.size();
			// int numBranchesToScan = rand.nextInt(numBranchesInPool + 1);
			int numBranchesToScan = 1;
			String[] branches = new String[numBranchesToScan];
			for (int i = 0; i < numBranchesToScan; i++) {
				int nextIndex = rand.nextInt(numBranchesInPool);
				branches[i] = branchPool.get(nextIndex);
			}
			return branches;
		}
	}

	// TODO: object re-use, should be thread local
	protected ReadOpData readOp;
	protected UpdateOpData updateOp;
	protected InsertOpData insertOp;
	protected DeleteOpData deleteOp;
	protected ScanOpData scanOp;
	protected MergeOpData mergeOp;
	protected BranchOpData branchOp;
	protected CompareOpData compareOp;

	public abstract void init(String startBranchRoot);

	public abstract TreeModOpData getNextForInit();

	public ReadOpData getNextForRead() {
		return new ReadOpData(getNextBranchNameForRead());
	}

	public UpdateOpData getNextForUpdate() {
		return new UpdateOpData(getNextBranchNameForUpdate());
	}

	public InsertOpData getNextForInsert() {
		return new InsertOpData(getNextBranchNameForInsert());
	}

	public DeleteOpData getNextForDelete() {
		return new DeleteOpData(getNextBranchNameForDelete());
	}

	public ScanOpData[] getNextForScan() {
		String[] branchesToScan = getNextBranchesForScan();
		ScanOpData[] ret = new ScanOpData[branchesToScan.length];
		for (int i = 0; i < branchesToScan.length; ++i) {
			String branch = branchesToScan[i];
			ScanOpData scanOp = new ScanOpData(branch);
			scanOp.setSubOperationName(getScanSubOperationName(branch));
			ret[i] = scanOp;
		}
		return ret;
	}

	protected String getScanSubOperationName(String branch) {
		return "SCANNED: (" + branch + ")";
	}

	public abstract CompareOpData[] getNextForCompare();

	public abstract MergeOpData getNextForMerge();

	public abstract BranchOpData getNextForBranch();

	protected abstract String[] getNextBranchesForScan();

	protected abstract String getNextBranchNameForInsert();

	protected abstract String getNextBranchNameForUpdate();

	protected abstract String getNextBranchNameForDelete();

	protected abstract String getNextBranchNameForRead();

	private int numBranches = 1;

	public String[] generateBranchNames(int n) {
		String[] out = new String[n];
		for (int i = 0; i < n; i++) {
			String newBranchName = BRANCH_NAME_BASE + numBranches;
			if (newBranchName.length() > MAX_BRANCH_NAME_LENGTH) {
				newBranchName = newBranchName.substring(0, MAX_BRANCH_NAME_LENGTH);
			}
			out[i] = newBranchName;
			++numBranches;
		}
		return out;
	}
}
