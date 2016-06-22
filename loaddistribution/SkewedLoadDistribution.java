package simpledb.versioned.benchmark.loaddistribution;

public class SkewedLoadDistribution extends LoadDistribution {

	int skewFactor;
	String masterBranchName;
	int numTuplesForMasterBranch;
	int numTuplesForOtherBranches;
	
	public SkewedLoadDistribution(int numInitialNewBranches, int numInitialTuples, int skewFactor, String masterBranchName) {
		super(numInitialNewBranches, numInitialTuples);
		this.skewFactor = skewFactor;
		this.masterBranchName = masterBranchName;
		setBranchTupleCounts();
	}
	
	private void setBranchTupleCounts(){
		int numBranches = getTotalInitialNumBranches();
		int numTuples = getTotalInitialNumTuples();
		int kDenom = (skewFactor + (numBranches-1));
		int numTuplesForFirstBranch = ((skewFactor*numTuples)/kDenom);
		int numTuplesForRest = (numTuples/kDenom);
		numTuplesForMasterBranch = numTuplesForFirstBranch;
		numTuplesForOtherBranches = numTuplesForRest;
	}
	
	@Override
	protected int getInitialNumTuplesToInsert(String branchName) {
		if(branchName.equals(masterBranchName)){
			return numTuplesForMasterBranch;
		}
		return numTuplesForOtherBranches;
	}

}
