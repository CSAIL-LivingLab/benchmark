package simpledb.versioned.benchmark.loaddistribution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class LoadDistribution {

	private final int numInitialTuples;
	private final int numInitialNewBranches;
	private final Map<String, Integer> branchNameToNumTuplesRemaining;
	private final Map<String, Integer> branchNameToNumTuplesInsertedUpdated;
	private final Map<String, Integer> branchNameToNumTuplesInsertedUpdatedSinceLastCommit;
	private volatile int totalNumTuplesInserted;

	public LoadDistribution(int numInitialNewBranches, int numInitialTuples) {
		this.numInitialNewBranches = numInitialNewBranches;
		this.numInitialTuples = numInitialTuples;
		branchNameToNumTuplesRemaining = new HashMap<>();
		branchNameToNumTuplesInsertedUpdated = new HashMap<>();
		branchNameToNumTuplesInsertedUpdatedSinceLastCommit = new HashMap<>();
		totalNumTuplesInserted = 0;
	}

	protected abstract int getInitialNumTuplesToInsert(String branchName);

	public void addBranch(String branchName) {
		int initialNumTuplesToInsertForBranch = getInitialNumTuplesToInsert(branchName);
		branchNameToNumTuplesRemaining.put(branchName, initialNumTuplesToInsertForBranch);
		branchNameToNumTuplesInsertedUpdated.put(branchName, 0);
		branchNameToNumTuplesInsertedUpdatedSinceLastCommit.put(branchName, 0);
	}

	public int getRemainingToInsert(String branchName) {
		return branchNameToNumTuplesRemaining.get(branchName);
	}

	public int getTotalInitialNumTuples() {
		return numInitialTuples;
	}

	public int getTotalInitialNumNewBranches() {
		return numInitialNewBranches;
	}

	public int getTotalInitialNumBranches() {
		return numInitialNewBranches + 1;
	}

	public int getTotalNumInserted() {
		return totalNumTuplesInserted;
	}

	public int getNumBranchesWithTuplesRemaining() {
		int num = 0;
		for (String branchName : branchNameToNumTuplesRemaining.keySet()) {
			int numRemaining = branchNameToNumTuplesRemaining.get(branchName);
			if (numRemaining > 0) {
				num++;
			}
		}
		return num;
	}

	public String[] getBranchesWithRemainingInserts() {
		List<String> branchesWithRemainingInserts = new ArrayList<String>();
		for (String branchName : branchNameToNumTuplesRemaining.keySet()) {
			int numRemaining = branchNameToNumTuplesRemaining.get(branchName);
			if (numRemaining > 0) {
				branchesWithRemainingInserts.add(branchName);
			}
		}
		return branchesWithRemainingInserts.toArray(new String[] {});
	}

	public int getNumInsertedUpdated(String branchName) {
		return branchNameToNumTuplesInsertedUpdated.get(branchName);
	}

	public void updateNumInsertedUpdated(String branchName, int numInserted) {
		totalNumTuplesInserted += numInserted;
		int currentNumInserted = branchNameToNumTuplesInsertedUpdated.get(branchName);
		branchNameToNumTuplesInsertedUpdated.put(branchName, currentNumInserted + numInserted);

		int currentNumInsertedSinceCommit = branchNameToNumTuplesInsertedUpdatedSinceLastCommit.get(branchName);
		branchNameToNumTuplesInsertedUpdatedSinceLastCommit.put(branchName,
				currentNumInsertedSinceCommit + numInserted);

		int oldNumRemaining = branchNameToNumTuplesRemaining.get(branchName);
		int tenativeNewNumRemaining = oldNumRemaining - numInserted;
		int newNumRemaining = tenativeNewNumRemaining < 0 ? 0 : tenativeNewNumRemaining;
		branchNameToNumTuplesRemaining.put(branchName, newNumRemaining);
	}

	public void resetNumTuplesInsertedUpdatedSinceLastCommit(String branchName) {
		branchNameToNumTuplesInsertedUpdatedSinceLastCommit.put(branchName, 0);
	}

	public int getNumTuplesInsertedUpdatedSinceLastCommit(String branchName) {
		return branchNameToNumTuplesInsertedUpdatedSinceLastCommit.get(branchName);
	}

	public int getTotalNumTuplesRemaining() {
		return Math.max(0, getTotalInitialNumTuples() - getTotalNumInserted());
	}

}
