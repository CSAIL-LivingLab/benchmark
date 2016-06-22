package simpledb.versioned.benchmark.tupleloadstrategy;

import java.util.Properties;

import simpledb.TupleDesc;

public class SingleTupleLoadStrategy extends AbstractTupleLoadStrategy{
	static final int NUM_TUPLES_FOR_OP = 1;

	public SingleTupleLoadStrategy(TupleDesc td, Properties p) {
		super(td, p);
	}

	@Override
	protected int getNumForInsert(String branchName) {
		return NUM_TUPLES_FOR_OP;
	}

	@Override
	protected int getNumForUpdate(String branchName) {
		return NUM_TUPLES_FOR_OP;
	}

	@Override
	protected int getNumForDelete(String branchName) {
		return NUM_TUPLES_FOR_OP;
	}

	@Override
	protected int getNumForRead(String branchName) {
		return NUM_TUPLES_FOR_OP;
	}

}
