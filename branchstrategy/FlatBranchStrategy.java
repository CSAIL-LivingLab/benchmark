package simpledb.versioned.benchmark.branchstrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import simpledb.Constants;

public class FlatBranchStrategy extends BranchStrategy {

	static final String MASTER_FIRST_CHILD_CASE_NAME = "MASTER_FIRST_CHILD";

	List<String> allBranches;
	String master;
	List<String> children;
	final Random insert, delete, update, read;

	public FlatBranchStrategy() {
		try {
			insert = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.FLATBS_SEED_MUL);
			delete = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.FLATBS_SEED_MUL);
			update = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.FLATBS_SEED_MUL);
			read = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.FLATBS_SEED_MUL);
		} catch (Exception ex) {
			throw new RuntimeException();
		}
	}

	@Override
	public void init(String startBranchRoot) {
		master = startBranchRoot;
		children = new ArrayList<String>();
		allBranches = new ArrayList<String>();
		allBranches.add(master);
	}

	@Override
	public TreeModOpData getNextForInit() {
		if (children.size() == 0) {
			return getNextForBranch();
		}
		return null;
	}

	@Override
	protected String[] getNextBranchesForScan() {
		if (children.size() == 0) {
			return new String[] { master };
		} else {
			return new String[] { children.get(0) };
		}
	}

	@Override
	protected String getNextBranchNameForInsert() {
		return getNextBranchNameForSingleBranchMod(insert);
	}

	@Override
	protected String getNextBranchNameForUpdate() {
		return getNextBranchNameForSingleBranchMod(update);
	}

	@Override
	protected String getNextBranchNameForDelete() {
		return getNextBranchNameForSingleBranchMod(delete);
	}

	@Override
	protected String getNextBranchNameForRead() {
		return getNextBranchNameForSingleBranchMod(read);
	}

	public String getNextBranchNameForSingleBranchMod(Random rand) {
		int numChildren = children.size();
		if (numChildren == 0) {
			return master;
		}
		
		int masterOrChild = rand.nextInt(numChildren + 1);
		if (masterOrChild == 0) {
			return master;
		}
		
		int childIndex = rand.nextInt(numChildren);
		return children.get(childIndex);
	}

	@Override
	public MergeOpData getNextForMerge() {
		throw new UnsupportedOperationException();
	}

	@Override
	public BranchOpData getNextForBranch() {
		String parent = master;
		String newBranchName = generateBranchNames(1)[0];
		children.add(newBranchName);
		allBranches.add(newBranchName);
		return new BranchOpData(parent, newBranchName);
	}

	@Override
	public CompareOpData[] getNextForCompare() {
		if (children.size() == 0) {
			throw new IllegalStateException();
		}
		
		String[] branches = new String[] { master, children.get(0) };
		CompareOpData out = new CompareOpData(branches);
		out.setSubOperationName(MASTER_FIRST_CHILD_CASE_NAME);
		return new CompareOpData[] { out };
	}

}