package simpledb.versioned.benchmark.branchstrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import simpledb.Constants;

public class DeepBranchStrategy extends BranchStrategy {

    static final String TAIL_HEAD_CASE_NAME = "TAIL_HEAD";
    static final String TAIL_PARENT_CASE_NAME = "TAIL_PARENT";

    List<String> branchesInOrder;
    int lastIndex;
    final Random insert, delete, update, read;

    public DeepBranchStrategy() {
	try {
	    insert = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEEPBS_SEED_MUL);
	    delete = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEEPBS_SEED_MUL);
	    update = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEEPBS_SEED_MUL);
	    read = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEEPBS_SEED_MUL);
	} catch (Exception ex) {
	    throw new RuntimeException();
	}
    }

    @Override
    public void init(String startRootBranch) {
	branchesInOrder = new ArrayList<String>();
	lastIndex = 0;
	branchesInOrder.add(startRootBranch);
    }

    @Override
    public TreeModOpData getNextForInit() {
	if (branchesInOrder.size() == 1) {
	    return getNextForBranch();
	}
	return null;
    }

    @Override
    protected String[] getNextBranchesForScan() {
	return new String[] { branchesInOrder.get(lastIndex) };
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
	return branchesInOrder.get(lastIndex);
    }

    @Override
    public MergeOpData getNextForMerge() {
	throw new UnsupportedOperationException();
    }

    @Override
    public BranchOpData getNextForBranch() {
	String parent = branchesInOrder.get(lastIndex);
	String newBranchName = generateBranchNames(1)[0];
	branchesInOrder.add(newBranchName);
	++lastIndex;
	return new BranchOpData(parent, newBranchName);
    }

    @Override
    public CompareOpData[] getNextForCompare() {
	if (branchesInOrder.size() < 2) {
	    throw new IllegalStateException();
	}

	String tail = branchesInOrder.get(lastIndex);
	String tailParent = branchesInOrder.get(lastIndex - 1);
	String head = branchesInOrder.get(0);

	CompareOpData tailParentCompare = new CompareOpData(new String[] { tailParent, tail });
	tailParentCompare.setSubOperationName(TAIL_PARENT_CASE_NAME);
	CompareOpData tailHeadCompare = new CompareOpData(new String[] { head, tail });
	tailHeadCompare.setSubOperationName(TAIL_HEAD_CASE_NAME);

	return new CompareOpData[] { tailParentCompare, tailHeadCompare };
    }

}
