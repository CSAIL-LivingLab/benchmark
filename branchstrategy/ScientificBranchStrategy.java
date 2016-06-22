package simpledb.versioned.benchmark.branchstrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import simpledb.Constants;
import simpledb.versioned.benchmark.ycsb.generator.DiscreteGenerator;

public class ScientificBranchStrategy extends BranchStrategy {

	/**
	 * Branch Compare Selection Parameters
	 */
	static final String TAIL_LATEST_ACTIVE_TAIL_MAINLINE_CASE_NAME = "TAIL_LATEST_ACTIVE_TAIL_MAINLINE";
	static final String TAIL_EARLIEST_ACTIVE_TAIL_MAINLINE_CASE_NAME = "TAIL_EARLIEST_ACTIVE_TAIL_MAINLINE";
	static final String TAIL_LATEST_ACTIVE_MAINLINE_BRANCH_POINT_NAME = "TAIL_LATEST_ACTIVE_MAINLINE_BRANCH_POINT";

	/**
	 * Branch Creation Selection Parameters.
	 */
	static final String EXTEND_MAINLINE_PROB_PROPERTY = "sci_extend_mainline_prob";
	static final String EXTEND_ACTIVE_PROB_PROPERTY = "sci_extend_active_prob";
	static final String CREATE_ACTIVE_FROM_MAIN_PROB_PROPERTY = "sci_create_active_from_mainline_prob";
	static final String CREATE_ACTIVE_FROM_ACTIVE_PROB_PROPERTY = "sci_create_active_from_active_prob";
	static final String END_ACTIVE_PROB_PROPERTY = "sci_end_active_prob";

	final double probabilityExtendMainline;
	final double probabilityExtendActive;
	final double probabilityCreateActiveFromActive;
	final double probabilityCreateActiveFromMainline;
	final double probabilityEndActive;

	class BranchLine {
		String mainelineForkedOff;
		List<String> branchesInActiveLine;

		public BranchLine(String mainelineForkedOff) {
			this.mainelineForkedOff = mainelineForkedOff;
			branchesInActiveLine = new ArrayList<String>();
		}
		
		public String getLatest() {
			return branchesInActiveLine.get(branchesInActiveLine.size() - 1);
		}
	}

	List<String> allBranches;
	List<String> mainlines;
	List<BranchLine> active;
	List<BranchLine> retired;

	final Random insert, delete, update, read, scan, compare, treeMod;
	final DiscreteGenerator branchOperationChooser;

	public ScientificBranchStrategy(double probabilityExtendMainline,
			double probabilityExtendActive,
			double probabilityCreateActiveFromActive,
			double probabilityCreateActiveFromMainline,
			double probabilityEndActive) {

		try {
			insert = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.SCIBS_SEED_MUL);
			delete = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.SCIBS_SEED_MUL);
			update = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.SCIBS_SEED_MUL);
			read = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.SCIBS_SEED_MUL);
			scan = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.SCIBS_SEED_MUL);
			compare = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.SCIBS_SEED_MUL);
			treeMod = new Random(Long.parseLong(System
					.getProperty(Constants.RNG_SEED))
					* Constants.SCIBS_SEED_MUL);
		} catch (Exception ex) {
			throw new RuntimeException();
		}

		this.probabilityExtendMainline = probabilityExtendMainline;
		this.probabilityExtendActive = probabilityExtendActive;
		this.probabilityCreateActiveFromActive = probabilityCreateActiveFromActive;
		this.probabilityCreateActiveFromMainline = probabilityCreateActiveFromMainline;
		this.probabilityEndActive = probabilityEndActive;

		branchOperationChooser = new DiscreteGenerator(treeMod);

		if (probabilityExtendMainline > 0) {
			branchOperationChooser.addValue(probabilityExtendMainline,
					EXTEND_MAINLINE_PROB_PROPERTY);
		}

		if (probabilityExtendActive > 0) {
			branchOperationChooser.addValue(probabilityExtendActive,
					EXTEND_ACTIVE_PROB_PROPERTY);
		}

		if (probabilityCreateActiveFromActive > 0) {
			branchOperationChooser.addValue(probabilityCreateActiveFromActive,
					CREATE_ACTIVE_FROM_ACTIVE_PROB_PROPERTY);
		}

		if (probabilityCreateActiveFromMainline > 0) {
			branchOperationChooser.addValue(
					probabilityCreateActiveFromMainline,
					CREATE_ACTIVE_FROM_MAIN_PROB_PROPERTY);
		}

		if (probabilityEndActive > 0) {
			branchOperationChooser.addValue(probabilityEndActive,
					END_ACTIVE_PROB_PROPERTY);
		}
	}

	@Override
	public void init(String startBranchRoot) {
		allBranches = new ArrayList<String>();
		allBranches.add(startBranchRoot);
		mainlines = new ArrayList<String>();
		mainlines.add(startBranchRoot);
		active = new ArrayList<BranchLine>();
		retired = new ArrayList<BranchLine>();
	}

	@Override
	public TreeModOpData getNextForInit() {
		if (active.size() == 0) {
			return getNextForBranch();
		}
		return null;
	}

	@Override
	protected String[] getNextBranchesForScan() {
		// Scan mainline head, earliest and latest actives
		
		List<String> branchesToScan = new ArrayList<String>();
		
		branchesToScan.add(mainlines.get(mainlines.size() - 1));
		
		if (active.size() > 0) {
			branchesToScan.add(active.get(0).getLatest());
		}
		
		if (active.size() > 1) {
			branchesToScan.add(active.get(active.size() - 1).getLatest());
		}
		
		String[] ret = new String[branchesToScan.size()];
		for (int i = 0; i < ret.length; ++i) {
			ret[i] = branchesToScan.get(i);
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
		String tenativeSelected = null;

		// select tail of an active or tail of mainline
		int tailOfMainlineIndex = mainlines.size() - 1;
		String tailOfMainline = mainlines.get(tailOfMainlineIndex);
		tenativeSelected = tailOfMainline;

		if (active.size() != 0) {
			int activeIndex = rand.nextInt(active.size());
			BranchLine selectedActive = active.get(activeIndex);
			int tailOfActiveIndex = selectedActive.branchesInActiveLine.size() - 1;
			String tailOfActiveBranch = selectedActive.branchesInActiveLine
					.get(tailOfActiveIndex);
			int activeOrMainline = rand.nextInt(2);
			tenativeSelected = activeOrMainline == 0 ? tailOfMainline
					: tailOfActiveBranch;
		}

		return tenativeSelected;
	}

	@Override
	public MergeOpData getNextForMerge() {
		// no merges, this is how this differs from dev
		throw new UnsupportedOperationException();
	}

	@Override
	public BranchOpData getNextForBranch() {
		String parent = null;
		String child = generateBranchNames(1)[0];

		while (parent == null) {
			String operation = branchOperationChooser.nextString();
			if (operation.equals(EXTEND_MAINLINE_PROB_PROPERTY)) {
				// extend mainline
				parent = mainlines.get(mainlines.size() - 1);

				// move mainline head
				mainlines.add(child);
			} else if (operation.equals(CREATE_ACTIVE_FROM_MAIN_PROB_PROPERTY)) {
				// select a branch point from mainline uniformly at random
				int mainlinePointToCreateFromIndex = treeMod.nextInt(mainlines
						.size());
				String mainlinePointToCreateFrom = mainlines
						.get(mainlinePointToCreateFromIndex);

				parent = mainlinePointToCreateFrom;

				// create new branch line
				BranchLine newBranchLine = new BranchLine(
						mainlinePointToCreateFrom);
				newBranchLine.branchesInActiveLine.add(child);
				active.add(newBranchLine);
			} else if (operation
					.equals(CREATE_ACTIVE_FROM_ACTIVE_PROB_PROPERTY)) {
				if (active.size() == 0) {
					continue;
				}

				// selective current active branch line to create from
				int activeIndex = treeMod.nextInt(active.size());
				BranchLine currentActiveBranchLine = active.get(activeIndex);

				int currentActiveBranchLineIndex = treeMod
						.nextInt(currentActiveBranchLine.branchesInActiveLine
								.size());
				String currentActiveBranchLineElementToCreateFrom = currentActiveBranchLine.branchesInActiveLine
						.get(currentActiveBranchLineIndex);

				parent = currentActiveBranchLineElementToCreateFrom;

				// create new branch line
				// need to pass along the mainline it was forked off to support
				// a later query
				BranchLine newBranchLine = new BranchLine(
						currentActiveBranchLine.mainelineForkedOff);
				newBranchLine.branchesInActiveLine.add(child);
				active.add(newBranchLine);
			} else if (operation.equals(EXTEND_ACTIVE_PROB_PROPERTY)) {
				if (active.size() == 0) {
					continue;
				}

				int activeIndex = treeMod.nextInt(active.size());
				BranchLine currentActiveBranchLine = active.get(activeIndex);

				// since we are extending the line the newBranchName takes the
				// place of the parent branch name so we can
				// continue to extend the line, we want the last one since this
				// is an extension
				int currentActiveBranchLineIndex = currentActiveBranchLine.branchesInActiveLine
						.size() - 1;
				String currentActiveBranchLineEndElement = currentActiveBranchLine.branchesInActiveLine
						.get(currentActiveBranchLineIndex);

				parent = currentActiveBranchLineEndElement;

				// take the end slot for later extensions
				currentActiveBranchLine.branchesInActiveLine.add(child);
			} else if (operation.equals(END_ACTIVE_PROB_PROPERTY)) {
				if (active.size() == 0) {
					continue;
				}
				int activeIndex = treeMod.nextInt(active.size());

				// move an active branch line into retired, it will not longer
				// be extended or used to create
				// other active lines
				retired.add(active.get(activeIndex));
				active.remove(activeIndex);
			}
		}
		allBranches.add(child);
		return new BranchOpData(parent, child);
	}

	@Override
	public CompareOpData[] getNextForCompare() {
		List<CompareOpData> compareOps = new ArrayList<CompareOpData>();
		
		String mainlineTail = mainlines.get(mainlines.size() - 1);
		
		if (active.size() == 0) {
			throw new IllegalStateException();
		}
		
		// Earliest active, mainline
		String earliestActive = active.get(0).getLatest();
		CompareOpData earliestActiveMainlineCompareOp = 
				new CompareOpData(new String[] { mainlineTail, earliestActive });
		earliestActiveMainlineCompareOp.setSubOperationName(TAIL_EARLIEST_ACTIVE_TAIL_MAINLINE_CASE_NAME);
		compareOps.add(earliestActiveMainlineCompareOp);
		
		if (active.size() > 1) {
			// Latest active, mainline
			String latestActive = active.get(active.size() - 1).getLatest();
			CompareOpData latestActiveMainlineCompareOp = 
					new CompareOpData(new String[] { mainlineTail, latestActive });
			latestActiveMainlineCompareOp.setSubOperationName(TAIL_LATEST_ACTIVE_TAIL_MAINLINE_CASE_NAME);
			compareOps.add(latestActiveMainlineCompareOp);
		}
		
		CompareOpData[] ret = new CompareOpData[compareOps.size()];
		for (int i = 0; i < ret.length; ++i) {
			ret[i] = compareOps.get(i);
		}
		
		return ret;
	}

}
