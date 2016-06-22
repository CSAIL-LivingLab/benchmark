package simpledb.versioned.benchmark.tupleloadstrategy;

import simpledb.versioned.benchmark.IntList;


public interface TupleLoadStrategy {
	IntList getNextForInsert(String branchName);

	IntList getNextForUpdate(String branchName);

	IntList getNextForDelete(String branchName);

	IntList getNextForRead(String branchName);

	void addBranch(String childBranchName, String... parentBranchNames);
}
