package simpledb.versioned.benchmark.loaddistribution;


public class UniformLoadDistribution extends LoadDistribution {
	
	int numTuplesPerBranch;
	public UniformLoadDistribution(int numInitialNewBranches, int numInitialTuples) {
		super(numInitialNewBranches, numInitialTuples);
		numTuplesPerBranch = (int) Math.ceil(numInitialTuples/((double)getTotalInitialNumBranches()));
	}

	@Override
	protected int getInitialNumTuplesToInsert(String branchName) {
		return numTuplesPerBranch;
	}

}
