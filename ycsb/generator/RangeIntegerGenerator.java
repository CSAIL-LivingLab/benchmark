package simpledb.versioned.benchmark.ycsb.generator;

public abstract class RangeIntegerGenerator extends IntegerGenerator {
	public abstract void updateRange(int lowerBound, int upperBound);

	public abstract int getUpperBound();
}
