package simpledb.versioned.benchmark.branchstrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import simpledb.Constants;
import simpledb.versioned.benchmark.ycsb.generator.DiscreteGenerator;

/**
 * Represents a software development type of graph. The ideas here are as
 * follows: -1 main branch -1 dev branch at a time -dev branch grows via a
 * sequence of feature branches that are short lived and branch off the dev
 * branch and then merge back into the dev branch to move the head of the dev
 * branch forward -main branch grows via: 1)hotfix branches that are short lived
 * and branch off the current head of mainline and then merge back into mainline
 * to move the head forward, 2)the dev branch that is merged into the current
 * head of the mainline branch to move mainline forward, the dev branch is then
 * immediately recreated by branching off the new head of mainline.
 * 
 * @author David
 * 
 */
public class DevBranchStrategy extends BranchStrategy {

	static final String DEV_TAIL_YOUNGEST_FEATURE_TAIL_CASE_NAME = "DEV_TAIL_YOUNGEST_FEATURE_TAIL";
	static final String DEV_TAIL_OLDEST_FEATURE_TAIL_CASE_NAME = "DEV_TAIL_OLDEST_FEATURE_TAIL";
	static final String DEV_TAIL_MAIN_TAIL_CASE_NAME = "DEV_TAIL_MAIN_TAIL";

	static final String START_HOT_FIX_OP_NAME = "START_HOTFIX";
	static final String START_FEATURE_OP_NAME = "START_FEATURE";

	static final String MERGE_DEV_OP_NAME = "MERGE_DEV";
	static final String MERGE_FEATURE_OP_NAME = "MERGE_FEATURE";
	static final String MERGE_HOTFIX_OP_NAME = "MERGE_HOTFIX";

	String mainline;
	String dev;
	List<String> activeFeatures;
	List<String> retiredFeatures;
	List<String> activeHotFixes;
	List<String> retiredHotFixes;

	String originalMainlineName;
	String originalDevName;

	final Random insert, delete, update, read, scan, compare, branch, merge;
	final DiscreteGenerator branchOperationChooser;
	final DiscreteGenerator mergeOperationChooser;

	public DevBranchStrategy(double mergeDevProb, double mergeFeatureProb, double mergeHotFixProb,
			double startFeatureProb, double startHotFixProb) {

		try {
			insert = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEVBS_SEED_MUL);
			delete = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEVBS_SEED_MUL);
			update = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEVBS_SEED_MUL);
			read = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEVBS_SEED_MUL);
			scan = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEVBS_SEED_MUL);
			compare = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEVBS_SEED_MUL);
			branch = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEVBS_SEED_MUL);
			merge = new Random(Long.parseLong(System.getProperty(Constants.RNG_SEED)) * Constants.DEVBS_SEED_MUL);
		} catch (Exception ex) {
			throw new RuntimeException();
		}

		branchOperationChooser = new DiscreteGenerator(branch);
		if (startHotFixProb > 0) {
			branchOperationChooser.addValue(startHotFixProb, START_HOT_FIX_OP_NAME);
		}
		if (startFeatureProb > 0) {
			branchOperationChooser.addValue(startFeatureProb, START_FEATURE_OP_NAME);
		}

		mergeOperationChooser = new DiscreteGenerator(merge);
		if (mergeDevProb > 0) {
			mergeOperationChooser.addValue(mergeDevProb, MERGE_DEV_OP_NAME);
		}
		if (mergeFeatureProb > 0) {
			mergeOperationChooser.addValue(mergeFeatureProb, MERGE_FEATURE_OP_NAME);
		}
		if (mergeHotFixProb > 0) {
			mergeOperationChooser.addValue(mergeHotFixProb, MERGE_HOTFIX_OP_NAME);
		}
	}

	@Override
	public void init(String startBranchRoot) {
		mainline = startBranchRoot;
		originalMainlineName = mainline;
		dev = null;
		activeFeatures = new ArrayList<String>();
		retiredFeatures = new ArrayList<String>();
		activeHotFixes = new ArrayList<String>();
		retiredHotFixes = new ArrayList<String>();
	}

	@Override
	public TreeModOpData getNextForInit() {
		if (dev == null) {
			return getNextForBranch();
		}
		return null;
	}

	@Override
	protected String[] getNextBranchesForScan() {
		List<String> retList = new ArrayList<String>();

		retList.add(mainline);

		if (dev != null) {
			retList.add(dev);
		}

		if (activeFeatures.size() > 0) {
			retList.add(activeFeatures.get(0));
		}

		if (activeFeatures.size() > 1) {
			retList.add(activeFeatures.get(activeFeatures.size() - 1));
		}

		if (activeHotFixes.size() > 0) {
			retList.add(activeHotFixes.get(0));
		}

		if (activeHotFixes.size() > 1) {
			retList.add(activeHotFixes.get(activeHotFixes.size() - 1));
		}

		String[] ret = new String[retList.size()];
		for (int i = 0; i < ret.length; ++i) {
			ret[i] = retList.get(i);
		}

		return ret;
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
		List<String> branchLineTails = new ArrayList<String>();

		branchLineTails.add(mainline);

		if (dev != null) {
			branchLineTails.add(dev);
		}

		int numActiveFeature = activeFeatures.size();
		int numActiveHotFix = activeHotFixes.size();
		if (numActiveFeature > 0) {
			int activeFeatureIndex = rand.nextInt(numActiveFeature);
			String selectedFeature = activeFeatures.get(activeFeatureIndex);
			branchLineTails.add(selectedFeature);
		}
		if (numActiveHotFix > 0) {
			int activeHotFixIndex = rand.nextInt(numActiveHotFix);
			String selectedActiveHotFix = activeHotFixes.get(activeHotFixIndex);
			branchLineTails.add(selectedActiveHotFix);
		}
		int index = rand.nextInt(branchLineTails.size());
		return branchLineTails.get(index);
	}

	@Override
	public BranchOpData getNextForBranch() {
		String parent, child;

		if (dev == null) {
			child = "dev";
			parent = mainline;
			dev = child;
			originalDevName = dev;
			return new BranchOpData(parent, child);
		}

		String nextOp = branchOperationChooser.nextString();
		if (nextOp.equals(START_FEATURE_OP_NAME)) {
			if (dev == null) {
				throw new IllegalStateException();
			}
			parent = dev;
			child = "feature_" + generateBranchNames(1)[0];
			activeFeatures.add(child);
			return new BranchOpData(parent, child);
		} else if (nextOp.equals(START_HOT_FIX_OP_NAME)) {
			if (mainline == null) {
				throw new IllegalStateException();
			}
			parent = mainline;
			child = "hotfix_" + generateBranchNames(1)[0];
			activeHotFixes.add(child);
			return new BranchOpData(parent, child);
		} else {
			throw new IllegalStateException();
		}
	}

	@Override
	public MergeOpData getNextForMerge() {

		// Entry 0 is the branch accepting the merge
		String[] parents = new String[2];
		String child = null;

		while (true) {
			String nextOp = mergeOperationChooser.nextString();
			if (nextOp.equals(MERGE_DEV_OP_NAME)) {
				// Given calls to getNextForInit, there should
				// always exist a reasonably full dev branch by
				// the time a merge operation is processed.
				if (dev == null) {
					throw new IllegalStateException();
				}
				parents[0] = mainline;
				parents[1] = dev;
				return new MergeOpData(parents);
			} else if (nextOp.equals(MERGE_FEATURE_OP_NAME)) {
				if (dev == null) {
					throw new IllegalStateException();
				} else if (activeFeatures.size() == 0) {
					// Move to next op. Might cause inf. loop
					// if dev merge prob is zero...
					continue;
				}

				int selectedFeatureIndex = merge.nextInt(activeFeatures.size());
				String selectedFeature = activeFeatures.get(selectedFeatureIndex);

				parents[0] = dev;
				parents[1] = selectedFeature;

				activeFeatures.remove(selectedFeatureIndex);
				retiredFeatures.add(selectedFeature);

				return new MergeOpData(parents);
			} else if (nextOp.equals(MERGE_HOTFIX_OP_NAME)) {
				if (mainline == null) {
					throw new IllegalStateException();
				} else if (activeHotFixes.size() == 0) {
					// Move to next op. Might cause inf. loop
					// if dev merge prob is zero...
					continue;
				}

				int selectedHotFixIndex = merge.nextInt(activeHotFixes.size());
				String selectedHotFix = activeHotFixes.get(selectedHotFixIndex);

				parents[0] = mainline;
				parents[1] = selectedHotFix;

				activeHotFixes.remove(selectedHotFixIndex);
				retiredHotFixes.add(selectedHotFix);

				return new MergeOpData(parents);
			} else {
				throw new IllegalStateException();
			}
		}
	}

	@Override
	public CompareOpData[] getNextForCompare() {
		if (dev == null) {
			throw new IllegalStateException();
		}

		List<CompareOpData> retList = new ArrayList<CompareOpData>();

		CompareOpData devMainlineCompareOp = new CompareOpData(new String[] { dev, mainline });
		devMainlineCompareOp.setSubOperationName(DEV_TAIL_MAIN_TAIL_CASE_NAME);
		retList.add(devMainlineCompareOp);

		if (activeFeatures.size() > 0) {
			String oldestActiveFeature = activeFeatures.get(0);
			CompareOpData devOldestFeatureCompareOp = new CompareOpData(new String[] { dev, oldestActiveFeature });
			devOldestFeatureCompareOp.setSubOperationName(DEV_TAIL_OLDEST_FEATURE_TAIL_CASE_NAME);
			retList.add(devOldestFeatureCompareOp);
		}

		if (activeFeatures.size() > 1) {
			String youngestActiveFeature = activeFeatures.get(activeFeatures.size() - 1);
			CompareOpData devYoungestFeatureCompareOp = new CompareOpData(new String[] { dev, youngestActiveFeature });
			devYoungestFeatureCompareOp.setSubOperationName(DEV_TAIL_YOUNGEST_FEATURE_TAIL_CASE_NAME);
			retList.add(devYoungestFeatureCompareOp);
		}

		// A few other options here, but let's start with these for now...

		CompareOpData[] ret = new CompareOpData[retList.size()];
		for (int i = 0; i < ret.length; ++i) {
			ret[i] = retList.get(i);
		}

		return ret;
	}

}