package simpledb.versioned.benchmark.ycsb;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import simpledb.DbException;
import simpledb.TransactionAbortedException;
import simpledb.TupleDesc;
import simpledb.versioned.BranchId;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy;
import simpledb.versioned.benchmark.tupleloadstrategy.TupleLoadStrategy;
import simpledb.versioned.benchmark.ycsb.VersionDB.TupleSrc;

public abstract class Scenario implements Cleanupable{
	public static class VersionedTableEntry{
		public final String tableName;
		public final String tableType;
		public VersionedTableEntry(String tableName, String tableType){
			this.tableName = tableName;
			this.tableType = tableType;
		}
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 * @param db 
	 * @param props 
	 * @throws IOException 
	 * @throws TransactionAbortedException 
	 * @throws DbException 
	 */
	public void init(Properties props, VersionDB db) throws DBException, IOException, DbException, TransactionAbortedException
	{
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
	}
	
	
	public abstract VersionedTableEntry getVersionTableEntry();
	
	public abstract TupleDesc getTupleDesc();

	public abstract BranchStrategy getBranchStrategy();

	public abstract TupleLoadStrategy getTupleLoadStrategy();
	
	public abstract String getRootBranchName();

}
