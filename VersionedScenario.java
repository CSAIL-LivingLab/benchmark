package simpledb.versioned.benchmark;

import java.io.IOException;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Logger;

import simpledb.BufferPool;
import simpledb.DbException;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.versioned.benchmark.branchstrategy.BranchStrategy;
import simpledb.versioned.benchmark.branchstrategy.DeepBranchStrategy;
import simpledb.versioned.benchmark.branchstrategy.DevBranchStrategy;
import simpledb.versioned.benchmark.branchstrategy.FlatBranchStrategy;
import simpledb.versioned.benchmark.branchstrategy.ScientificBranchStrategy;
import simpledb.versioned.benchmark.loaddistribution.LoadDistribution;
import simpledb.versioned.benchmark.loaddistribution.SkewedLoadDistribution;
import simpledb.versioned.benchmark.loaddistribution.UniformLoadDistribution;
import simpledb.versioned.benchmark.tupleloadstrategy.ClusteredTupleLoadStrategy;
import simpledb.versioned.benchmark.tupleloadstrategy.SingleTupleLoadStrategy;
import simpledb.versioned.benchmark.tupleloadstrategy.TupleLoadStrategy;
import simpledb.versioned.benchmark.ycsb.DBException;
import simpledb.versioned.benchmark.ycsb.Scenario;
import simpledb.versioned.benchmark.ycsb.VersionDB;
import simpledb.versioned.benchmark.ycsb.VersionDB.MergeProc;
import simpledb.versioned.benchmark.ycsb.VersionDB.TupleSrc;

public class VersionedScenario extends Scenario {
	static final Logger logger = Logger.getLogger(VersionedScenario.class);

	static final String NOTE_SYMBOL = "*";

	static final String COLUMN_NAME_BASE = "column";

	static final String EXISTING_FILE_PROPERTY = "file_name";

	static final String TABLE_NAME_PROPERTY = "tbl_name";
	static final String DEFAULT_TABLE_NAME = "versioned_table";

	static final String MERGE_PROC_PROPERTY = "merge_proc";
	static final String MERGE_PROC_DEFAULT = VersionDB.MergeProc.THREE_WAY.getName();

	static final String TABLE_TYPE_PROPERTY = "tbl_type";
	static final String TABLE_TYPE_PROPERTY_DEFAULT = "none";

	static final String NUM_COLUMNS_PROPERTY = "num_cols";
	static final String DEFAULT_NUM_COLUMNS = "15";

	static final String TUPLE_SRC_PROPERTY = "tuple_src";
	static final String TUPLE_SRC_DEFAULT = "single";

	static final String TUPLE_SRC_FILE_NAME_PROPERTY = "tuple_src_file_name";

	static final String NUM_VERSIONS_PROPERTY = "load_num_versions";
	static final String DEFAULT_NUM_VERSIONS = "20";

	static final String NUM_TUPLES_LD_PROPERTY = "load_num_tuples";
	static final String NUM_TUPLES_LD_DEFAULT = "1000";

	static final String NUM_INSERTS_UPDATES_IN_BRANCH_BEFORE_COMMIT_PROPERTY = "num_inserts_updates_in_branch_before_commit";
	static final String NUM_INSERTS_UPDATES_IN_BRANCH_BEFORE_COMMIT_DEFAULT = "100000000";

	static final String BRANCH_PROPORTION_PROPERTY = "load_branch_proportion";
	static final String BRANCH_PROPORTION_DEFAULT = "1.0";

	static final String MERGE_PROPORTION_PROPERTY = "load_merge_proportion";
	static final String MERGE_PROPORTION_DEFAULT = "0.0";

	static final String UPDATE_PROBABILITY_PROPERTY = "load_update_probability";
	static final String UPDATE_PROBABILITY_DEFAULT = "0.0";

	/**
	 * Fraction of inserts designated for a branch that should be inserted into
	 * it when it is first created.
	 */
	static final String BRANCH_SPACING_LD_PROPERTY = "load_branch_spacing";
	static final String BRANCH_SPACING_LD_DEFAULT = "1";

	static final int NUM_TUPLES_BUFFER = 1000;

	static final String BRANCH_START_ROOT_PROPERTY = "branch_root";
	static final String DEFAULT_BRANCH_START_ROOT = "master";

	/**
	 * BufferPool params.
	 */
	static final String NUM_PAGES_PROPERTY = "num_pages";
	static final String NUM_PAGES_DEFAULT = "100";
	static final String PAGE_SIZE_PROPERTY = "page_size";
	static final String PAGE_SIZE_DEFAULT = Integer.toString(BufferPool.getPageSize());

	/**
	 * Graph structure.
	 */
	static final String BRANCHING_STRATEGY_PROPERTY = "branching_strategy";
	static final String BRANCHING_STRATEGY_DEFAULT = "flat";

	/**
	 * Load distribution.
	 */
	static final String LOAD_DISTRIBUTION_PROPERTY = "ld_distribution_name";
	static final String LOAD_DISTRIBUTION_DEFAULT = "uniform";

	static final String LOAD_SKEW_PROPERTY = "ld_skew_factor";
	static final String LOAD_SKEW_DEFAULT = "10";

	static final String REMOVE_BRANCH_CAP_PROPERTY = "remove_branch_cap";
	static final String REMOVE_BRANCH_CAP_DEFAULT = "false";

	/**
	 * Tuple Load Pattern used in build phase and workload phase to enforce
	 * certain physical properties when an insert or update is requested.
	 */
	static final String TUPLE_LOAD_STRATEGY_PROPERTY = "tuple_ld_strategy_name";
	static final String TUPLE_LOAD_STRATEGY_DEFAULT = "single";
	static final String FRACTION_OF_PAGE_TO_CLUSTER_PROPERTY = "page_custer_fraction";
	static final String FRACTION_OF_PAGE_TO_CLUSTER_DEFAULT = "1";

	/**
	 * Scientific Params.
	 */
	static final String EXTEND_MAINLINE_PROB_PROPERTY = "sci_extend_mainline_prob";
	static final String EXTEND_MAINLINE_PROB_DEFAULT = "0.0";
	static final String EXTEND_ACTIVE_PROB_PROPERTY = "sci_extend_active_prob";
	static final String EXTEND_ACTIVE_PROB_DEFAULT = "0.0";
	static final String CREATE_ACTIVE_FROM_MAIN_PROB_PROPERTY = "sci_create_active_from_mainline_prob";
	static final String CREATE_ACTIVE_FROM_MAIN_PROB_DEFAULT = "0.0";
	static final String CREATE_ACTIVE_FROM_ACTIVE_PROB_PROPERTY = "sci_create_active_from_active_prob";
	static final String CREATE_ACTIVE_FROM_ACTIVE_PROB_DEFAULT = "0.0";
	static final String END_ACTIVE_PROB_PROPERTY = "sci_end_active_prob";
	static final String END_ACTIVE_PROB_DEFAULT = "0.0";

	/**
	 * Dev Params.
	 */
	private static final String MERGE_DEV_PROB_PROPERTY = "merge_dev_prob";
	private static final String MERGE_DEV_PROB_PROPERTY_DEFAULT = ".20";
	private static final String MERGE_FEATURE_PROB_PROPERTY = "merge_feature_prob";
	private static final String MERGE_FEATURE_PROB_PROPERTY_DEFAULT = ".40";
	private static final String MERGE_HOTFIX_PROB_PROPERTY = "merge_hotfix_prob";
	private static final String MERGE_HOTFIX_PROB_PROPERTY_DEFAULT = ".40";
	private static final String START_FEATURE_PROB_PROPERTY = "start_feature_prob";
	private static final String START_FEATURE_PROB_DEFAULT = ".75";
	private static final String START_HOTFIX_PROB_PROPERTY = "start_hotfix_prob";
	private static final String START_HOTFIX_PROB_DEFAULT = ".25";

	/**
	 * Random Params.
	 */
	static final String BRANCH_DISTRIBUTION_PROPERTY = "rand_branch_dist";
	static final String BRANCH_DISTRIBUTION_DEFAULT = "uniform";

	public static enum TupleSrcType {
		SINGLE("single"), MULTIPLE_GEN("mult_gen"), MULTIPlE_FILE("mult_file");
		private final String name;

		TupleSrcType(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	public static enum VersionGraphStructure {
		DEEP("deep"), FLAT("flat"), SCIENTIFIC("scientific"), DEV("dev"), RANDOM("rand");

		private final String name;

		VersionGraphStructure(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	};

	public static enum LoadDistributionType {
		UNIFORM("uniform"), SKEW("skew");

		private final String name;

		LoadDistributionType(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	};

	public static enum TupleLoadStrategyType {
		SINGLE("single"), CLUSTERED("clustered");

		private final String name;

		TupleLoadStrategyType(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	public static class SingleTupleTupleSrc implements TupleSrc {

		final Tuple tup;

		public SingleTupleTupleSrc(TupleDesc td) {
			tup = new Tuple(td);
		}

		@Override
		public void open(TransactionId tid) throws DbException, TransactionAbortedException {
			// do nothing on purpose

		}

		@Override
		public void rewind() {
			// do nothing on purpose

		}

		@Override
		public void close() {
			// do nothing on purpose

		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Tuple next() {
			tup.setRecordId(null);
			return tup;
		}
	}

	private TupleDesc td;
	private VersionedTableEntry tableEntry;
	private String startBranchRoot;
	private VersionDB db;
	private BranchStrategy branchStrategy;
	private TupleLoadStrategy tupleLoadStrategy;
	private LoadDistribution loadDistribution;

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 * 
	 * @param db
	 * @param props
	 * @throws IOException
	 * @throws TransactionAbortedException
	 * @throws DbException
	 */
	@Override
	public void init(Properties props, VersionDB db)
			throws DBException, IOException, DbException, TransactionAbortedException {
		/**
		 * TODO: 1) figure out fine grained way to control
		 * inserts/updates/deletes more than the random natrue of the workload
		 * i.e. want a mix of isert/update/delete but need to insert X GB of
		 * data and possibly achieve a level of page clustering or uniformly
		 * distribute the inserts of the branches. How to do this is still not
		 * clear to me.
		 */
		logger.info("Setting up...");

		String list = "Parameter list:\n";
		for (Entry<Object, Object> p : props.entrySet()) {
			list += p.getKey() + "=" + p.getValue() + "\n";
		}
		logger.info(list);
		logger.info("Loading Arguments!");

		long startTime = new Date().getTime();

		// set db
		this.db = db;

		// set table names for each type of table
		String tableName = props.getProperty(TABLE_NAME_PROPERTY, DEFAULT_TABLE_NAME);
		String tableType = props.getProperty(TABLE_TYPE_PROPERTY, TABLE_TYPE_PROPERTY_DEFAULT);

		if (!db.getVersionDBFileTypes().contains(tableType)) {
			logger.fatal("Invalid table type: " + tableType);
			throw new IllegalArgumentException("Invalid table type: " + tableType);
		}

		// get basic table and system data
		tableEntry = new VersionedTableEntry(tableName, tableType);
		MergeProc mergeProc = getMergeProc(props.getProperty(MERGE_PROC_PROPERTY, MERGE_PROC_DEFAULT));

		logger.info("Table Type is: " + tableType);
		logger.info("Merge Procedure is: " + mergeProc);

		int numColumns = Integer.parseInt(props.getProperty(NUM_COLUMNS_PROPERTY, DEFAULT_NUM_COLUMNS));
		td = TupleDesc.generateIntTupleDesc(COLUMN_NAME_BASE, numColumns);
		int numPages = Integer.parseInt(props.getProperty(NUM_PAGES_PROPERTY, NUM_PAGES_DEFAULT));
		int pageSize = Integer.parseInt(props.getProperty(PAGE_SIZE_PROPERTY, PAGE_SIZE_DEFAULT));
		startBranchRoot = (String) props.getOrDefault(BRANCH_START_ROOT_PROPERTY, DEFAULT_BRANCH_START_ROOT);

		// get WORKLOAD and BUILD PHASE arguments (these are used in build phase
		// and passed along to workload phase)
		TupleLoadStrategyType tupleLoadStrategyType = getTupleLoadStrategyType(
				(String) props.getOrDefault(TUPLE_LOAD_STRATEGY_PROPERTY, TUPLE_LOAD_STRATEGY_DEFAULT));
		VersionGraphStructure graphStructure = getVersionGraphStructure(
				(String) props.getOrDefault(BRANCHING_STRATEGY_PROPERTY, BRANCHING_STRATEGY_DEFAULT));

		switch (graphStructure) {
		case DEEP:
			branchStrategy = new DeepBranchStrategy();
			break;
		case FLAT:
			branchStrategy = new FlatBranchStrategy();
			break;
		case DEV:
			double mergeDevProb = Double
					.parseDouble((String) props.getOrDefault(MERGE_DEV_PROB_PROPERTY, MERGE_DEV_PROB_PROPERTY_DEFAULT));
			double mergeFeatureProb = Double.parseDouble(
					(String) props.getOrDefault(MERGE_FEATURE_PROB_PROPERTY, MERGE_FEATURE_PROB_PROPERTY_DEFAULT));
			double mergeHotFixProb = Double.parseDouble(
					(String) props.getOrDefault(MERGE_HOTFIX_PROB_PROPERTY, MERGE_HOTFIX_PROB_PROPERTY_DEFAULT));
			double startFeatureProb = Double
					.parseDouble((String) props.getOrDefault(START_FEATURE_PROB_PROPERTY, START_FEATURE_PROB_DEFAULT));
			double startHotFixProb = Double
					.parseDouble((String) props.getOrDefault(START_HOTFIX_PROB_PROPERTY, START_HOTFIX_PROB_DEFAULT));
			branchStrategy = new DevBranchStrategy(mergeDevProb, mergeFeatureProb, mergeHotFixProb, startFeatureProb,
					startHotFixProb);
			break;
		case SCIENTIFIC:
			// TODO: tie these to an input stream
			double probabilityExtendMainline = Double.parseDouble(
					(String) props.getOrDefault(EXTEND_MAINLINE_PROB_PROPERTY, EXTEND_MAINLINE_PROB_DEFAULT));
			double probabilityExtendActive = Double
					.parseDouble((String) props.getOrDefault(EXTEND_ACTIVE_PROB_PROPERTY, EXTEND_ACTIVE_PROB_DEFAULT));
			double probabilityCreateActiveFromActive = Double.parseDouble((String) props
					.getOrDefault(CREATE_ACTIVE_FROM_ACTIVE_PROB_PROPERTY, CREATE_ACTIVE_FROM_ACTIVE_PROB_DEFAULT));
			double probabilityCreateActiveFromMainline = Double.parseDouble((String) props
					.getOrDefault(CREATE_ACTIVE_FROM_MAIN_PROB_PROPERTY, CREATE_ACTIVE_FROM_MAIN_PROB_DEFAULT));
			double probabilityEndActive = Double
					.parseDouble((String) props.getOrDefault(END_ACTIVE_PROB_PROPERTY, END_ACTIVE_PROB_DEFAULT));
			branchStrategy = new ScientificBranchStrategy(probabilityExtendMainline, probabilityExtendActive,
					probabilityCreateActiveFromActive, probabilityCreateActiveFromMainline, probabilityEndActive);
			break;
		default:
			throw new IllegalStateException();
		}

		logger.info("Selected Branch Strategy: " + graphStructure);

		switch (tupleLoadStrategyType) {
		case SINGLE:
			tupleLoadStrategy = new SingleTupleLoadStrategy(td, props);
			break;
		case CLUSTERED:
			double fractionOfPageToCluster = Double.parseDouble((String) props
					.getOrDefault(FRACTION_OF_PAGE_TO_CLUSTER_PROPERTY, FRACTION_OF_PAGE_TO_CLUSTER_DEFAULT));
			tupleLoadStrategy = new ClusteredTupleLoadStrategy(td, props, pageSize, fractionOfPageToCluster);
			break;
		}

		logger.info("Selected Tuple Load Strategy: " + tupleLoadStrategyType);

		// get BUILD PHASE construction arguments
		int numTuplesLoad = Integer
				.parseInt((String) props.getOrDefault(NUM_TUPLES_LD_PROPERTY, NUM_TUPLES_LD_DEFAULT));
		int numVersions = Integer.parseInt((String) props.getOrDefault(NUM_VERSIONS_PROPERTY, DEFAULT_NUM_VERSIONS));
		TupleSrcType tupleSrcType = getTupleSrcType((String) props.getOrDefault(TUPLE_SRC_PROPERTY, TUPLE_SRC_DEFAULT));
		LoadDistributionType initialLoadDistribution = getLoadDistributionType(
				(String) props.getOrDefault(LOAD_DISTRIBUTION_PROPERTY, LOAD_DISTRIBUTION_DEFAULT));
		double branchSpacingFactor = Double
				.parseDouble((String) props.getOrDefault(BRANCH_SPACING_LD_PROPERTY, BRANCH_SPACING_LD_DEFAULT));

		TupleSrc tupleSrc = null;
		switch (tupleSrcType) {
		case SINGLE:
			tupleSrc = new SingleTupleTupleSrc(td);
			break;
		case MULTIPLE_GEN:
			int numTuplesNeeded = numTuplesLoad + NUM_TUPLES_BUFFER;
			tupleSrc = db.getNewTupleSrc(td, numTuplesNeeded);
			break;
		case MULTIPlE_FILE:
			String tupleSrcFileName = props.getProperty(TUPLE_SRC_FILE_NAME_PROPERTY);
			tupleSrc = db.getTupleSrcFromFile(tupleSrcFileName, td);
			break;
		}

		logger.info("Generated Tuple Src: " + tupleSrcType);

		switch (initialLoadDistribution) {
		case UNIFORM:
			loadDistribution = new UniformLoadDistribution(numVersions, numTuplesLoad);
			break;
		case SKEW:
			int loadSkew = Integer.parseInt((String) props.getOrDefault(LOAD_SKEW_PROPERTY, LOAD_SKEW_DEFAULT));
			loadDistribution = new SkewedLoadDistribution(numVersions, numTuplesLoad, loadSkew, startBranchRoot);
			break;
		}

		// on top of the fixed number of inserts, we will also probabilistically
		// do updates

		double initialUpdateProbability = Double
				.parseDouble(props.getProperty(UPDATE_PROBABILITY_PROPERTY, UPDATE_PROBABILITY_DEFAULT));

		logger.info("Initial Update Probability during preliminary tree construction: " + initialUpdateProbability);

		double initialBranchProportion = Double
				.parseDouble(props.getProperty(BRANCH_PROPORTION_PROPERTY, BRANCH_PROPORTION_DEFAULT));
		double initialMergeProportion = Double
				.parseDouble(props.getProperty(MERGE_PROPORTION_PROPERTY, MERGE_PROPORTION_DEFAULT));

		logger.info("Initial Branch Propoartion during preliminary tree construction: " + initialBranchProportion);
		logger.info("Initial Merge Propoartion during preliminary tree construction: " + initialMergeProportion);

		boolean removeBranchCap = Boolean
				.parseBoolean(props.getProperty(REMOVE_BRANCH_CAP_PROPERTY, REMOVE_BRANCH_CAP_DEFAULT));

		logger.info("Remove per-branch capacity limit: " + removeBranchCap);

		int numInsertsUpdatesIntoBranchBeforeCommit = Integer
				.parseInt((String) props.getOrDefault(NUM_INSERTS_UPDATES_IN_BRANCH_BEFORE_COMMIT_PROPERTY,
						NUM_INSERTS_UPDATES_IN_BRANCH_BEFORE_COMMIT_DEFAULT));

		logger.info("Num inserts/updates into branch before commit in that branch: "
				+ numInsertsUpdatesIntoBranchBeforeCommit);

		// COMMENCE BUILD PHASE
		logger.info("Commencing Build Phase!");

		logger.info("Resetting BufferPool.");

		// reset buffer pool
		db.setPageSize(pageSize);
		db.setNumBufferPoolPages(numPages);

		// start build phase transaction
		TransactionId tidT = db.startTransaction();

		logger.info("Creating table.");

		// create the table
		db.createVersionedTable(tidT, tableEntry.tableName, td, tableEntry.tableType, mergeProc);

		logger.info("Creating and loading version tree for table.");

		// build the tree
		VersionGraphBuilder builder = new VersionGraphBuilder(db, tableEntry, tidT, td, startBranchRoot, branchStrategy,
				loadDistribution, tupleLoadStrategy, tupleSrc, initialBranchProportion, initialMergeProportion,
				initialUpdateProbability, branchSpacingFactor, removeBranchCap,
				numInsertsUpdatesIntoBranchBeforeCommit);
		builder.build();

		// finish setup, commit
		db.commitTransaction(tidT);

		logger.info("Total Num Tuples Inserted: " + loadDistribution.getTotalNumInserted());
		logger.info("Total amount of data inserted (MB): "
				+ (loadDistribution.getTotalNumInserted() * td.getSize()) / Math.pow(10, 6));

		logger.info("Setup done!");

		long endTime = new Date().getTime();

		logger.info("Setup took (ms): " + (endTime - startTime));
	}

	private LoadDistributionType getLoadDistributionType(String loadDistributionName) {
		for (LoadDistributionType blp : LoadDistributionType.values()) {
			if (blp.getName().equals(loadDistributionName)) {
				return blp;
			}
		}
		throw new IllegalArgumentException("Invalid branch load pattern!");
	}

	private VersionGraphStructure getVersionGraphStructure(String graphName) {
		for (VersionGraphStructure vgs : VersionGraphStructure.values()) {
			if (vgs.getName().equals(graphName)) {
				return vgs;
			}
		}
		throw new IllegalArgumentException("Invalid version graph structure!");
	}

	private MergeProc getMergeProc(String mpName) {
		for (MergeProc mp : VersionDB.MergeProc.values()) {
			if (mp.getName().equals(mpName)) {
				return mp;
			}
		}
		throw new IllegalArgumentException("Invalid merge proc!");
	}

	private TupleSrcType getTupleSrcType(String tupleSrcTypeName) {
		for (TupleSrcType tst : TupleSrcType.values()) {
			if (tst.getName().equals(tupleSrcTypeName)) {
				return tst;
			}
		}
		throw new IllegalArgumentException("Invalid tuple source type!");
	}

	private TupleLoadStrategyType getTupleLoadStrategyType(String tupleLoadStrategyName) {
		for (TupleLoadStrategyType tls : TupleLoadStrategyType.values()) {
			if (tls.getName().equals(tupleLoadStrategyName)) {
				return tls;
			}
		}
		throw new IllegalArgumentException("Invalid tuple load strategy!");
	}

	@Override
	public TupleDesc getTupleDesc() {
		return td;
	}

	@Override
	public BranchStrategy getBranchStrategy() {
		return branchStrategy;
	}

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException {
		logger.info("Cleaning up!");
		if (db != null) {
			db.deleteTable(tableEntry.tableName);
		}

	}

	@Override
	public String getRootBranchName() {
		return startBranchRoot;
	}

	@Override
	public VersionedTableEntry getVersionTableEntry() {
		return tableEntry;
	}

	@Override
	public TupleLoadStrategy getTupleLoadStrategy() {
		return tupleLoadStrategy;
	}
}
