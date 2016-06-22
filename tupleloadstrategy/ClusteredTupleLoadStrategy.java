package simpledb.versioned.benchmark.tupleloadstrategy;

import java.util.Properties;

import simpledb.TupleDesc;

public class ClusteredTupleLoadStrategy extends AbstractTupleLoadStrategy {
	protected static final int COMPENSATING_FACTOR = 0;

	private final double fractionOfPageToCluster;
	private final int tuplesPerPage;
	private final int numTuplesForUpdateInsert;
	private final int numTuplesForDelete;
	private final int numTuplesForRead;

	public ClusteredTupleLoadStrategy(TupleDesc td, Properties p, int pageSize,
			double fractionOfPageToCluster) {
		super(td, p);
		this.fractionOfPageToCluster = fractionOfPageToCluster;
		tuplesPerPage = (int) Math.ceil(pageSize
				/ ((double) td.getSize() + COMPENSATING_FACTOR));
		numTuplesForUpdateInsert = Math.max(
				(int) (fractionOfPageToCluster * tuplesPerPage), 1);
		// TODO: maybe this should be clustered to?
		numTuplesForDelete = 1;
		numTuplesForRead = 1;

	}

	@Override
	protected int getNumForInsert(String branchName) {
		return numTuplesForUpdateInsert;
	}

	@Override
	protected int getNumForUpdate(String branchName) {
		return numTuplesForUpdateInsert;
	}

	@Override
	protected int getNumForDelete(String branchName) {
		return numTuplesForDelete;
	}

	@Override
	protected int getNumForRead(String branchName) {
		return numTuplesForRead;
	}
}
