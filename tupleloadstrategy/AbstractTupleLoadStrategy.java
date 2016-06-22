package simpledb.versioned.benchmark.tupleloadstrategy;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import simpledb.TupleDesc;
import simpledb.Type;
import simpledb.versioned.benchmark.IntList;
import simpledb.versioned.benchmark.LinkedIntSetDistribution;
import simpledb.versioned.benchmark.ycsb.generator.CounterGenerator;
import simpledb.versioned.benchmark.ycsb.generator.RangeIntegerGenerator;
import simpledb.versioned.benchmark.ycsb.generator.UniformIntegerGenerator;

public abstract class AbstractTupleLoadStrategy implements TupleLoadStrategy {
    public enum KeyDistribution {
	UNIFORM("uniform");
	private String name;

	KeyDistribution(String name) {
	    this.name = name;
	}

	public String getName() {
	    return name;
	}
    };

    /**
     * KEY ACCESS PARAMS
     */

    /**
     * The name of the property for the the distribution of requests across the
     * keyspace. Options are "uniform", "zipfian" and "latest"
     */
    public static final String KEY_REQUEST_DISTRIBUTION_PROPERTY = "key_requestdistribution";

    /**
     * The default distribution of requests across the keyspace
     */
    public static final String KEY_REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * Percentage data items that constitute the hot set.
     */
    public static final String KEY_HOTSPOT_DATA_FRACTION = "key_hotspotdatafraction";

    /**
     * Default value of the size of the hot set.
     */
    public static final String KEY_HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

    /**
     * Percentage operations that access the hot set.
     */
    public static final String KEY_HOTSPOT_OPN_FRACTION = "key_hotspotopnfraction";

    /**
     * Default value of the percentage operations accessing the hot set.
     */
    public static final String KEY_HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

    class BranchData {
	final String branchName;
	final LinkedIntSetDistribution keys;

	public BranchData(String branchName) {
	    this.branchName = branchName;
	    keys = new LinkedIntSetDistribution(getKeyDistribution());
	}

	public void addAll(LinkedIntSetDistribution newKeys) {
	    keys.addAll(newKeys);
	}

	public void addAll(IntList newKeys) {
	    keys.addAll(newKeys);
	}

	public void removeAll(IntList existingKeysToRemove) {
	    keys.removeAll(existingKeysToRemove);
	}
    }

    Map<String, BranchData> branchNameToBranchData;
    final TupleDesc td;
    final Type primaryKeyType;
    final int primaryKeyIndex;
    CounterGenerator insertKeyGenerator;
    Properties p;
    // should be thread local
    IntList outList;

    public AbstractTupleLoadStrategy(TupleDesc td, Properties p) {
	this.td = td;
	primaryKeyIndex = td.getPrimaryKeyIndex();
	primaryKeyType = td.getFieldType(primaryKeyIndex);
	insertKeyGenerator = new CounterGenerator(0);
	branchNameToBranchData = new HashMap<String, BranchData>();
	this.p = p;
	switch (primaryKeyType) {
	case INT_TYPE:
	    break;
	default:
	    throw new IllegalArgumentException("Primary key must be an int!");
	}
	outList = new IntList();
    }

    private RangeIntegerGenerator getKeyDistribution() {
	String keyRequestDistribution = p.getProperty(KEY_REQUEST_DISTRIBUTION_PROPERTY,
		KEY_REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
	KeyDistribution keyDistribution = getKeyDistributionType(keyRequestDistribution);

	RangeIntegerGenerator updateKeychooser = null;
	switch (keyDistribution) {
	case UNIFORM:
	    updateKeychooser = new UniformIntegerGenerator(0, 0);
	    break;
	default:
	    throw new IllegalArgumentException("Invalid distribution specified!");
	}
	return updateKeychooser;
    }

    @Override
    public void addBranch(String childBranchName, String... parentBranchNames) {
	if (branchNameToBranchData.containsKey(childBranchName)) {
	    throw new IllegalArgumentException("Branch already exists: " + childBranchName);
	}
	BranchData newBranchData = new BranchData(childBranchName);

	// should add to set in backwards fashion so that higher precedence
	// branches overwrite the effects of lower precedence branches, but
	// doing it the way it is below is ok since
	// just taking the union of keys that exist in all the
	// branches (don't have to deal with conflicting record ids)
	for (String parentBranchName : parentBranchNames) {
	    BranchData branchData = getBranchData(parentBranchName);
	    newBranchData.addAll(branchData.keys);
	}

	branchNameToBranchData.put(childBranchName, newBranchData);
    }

    private BranchData getBranchData(String branchName) {
	if (!branchNameToBranchData.containsKey(branchName)) {
	    throw new IllegalArgumentException("Branch does not exist: " + branchName);
	}
	return branchNameToBranchData.get(branchName);
    }

    @Override
    public IntList getNextForInsert(String branchName) {
	outList.clear();
	int numKeys = getNumForInsert(branchName);
	for (int i = 0; i < numKeys; i++) {
	    int nextInt = insertKeyGenerator.nextInt();
	    outList.add(nextInt);
	}
	BranchData branchData = getBranchData(branchName);
	branchData.addAll(outList);
	return outList;
    }

    @Override
    public IntList getNextForUpdate(String branchName) {
	int numKeys = getNumForUpdate(branchName);
	IntList keysForUpdate = getKeysForModifyOperation(branchName, numKeys);
	return keysForUpdate;
    }

    @Override
    public IntList getNextForDelete(String branchName) {
	int numKeys = getNumForDelete(branchName);
	IntList keysForDelete = getKeysForModifyOperation(branchName, numKeys);
	BranchData branchData = getBranchData(branchName);
	branchData.removeAll(keysForDelete);
	return keysForDelete;
    }

    @Override
    public IntList getNextForRead(String branchName) {
	int numKeys = getNumForRead(branchName);
	return getKeysForModifyOperation(branchName, numKeys);
    }

    private IntList getKeysForModifyOperation(String branchName, int numKeys) {
	outList.clear();
	BranchData branchData = getBranchData(branchName);
	branchData.keys.getRandomKeys(outList, numKeys);
	return outList;
    }

    protected abstract int getNumForInsert(String branchName);

    protected abstract int getNumForUpdate(String branchName);

    protected abstract int getNumForDelete(String branchName);

    protected abstract int getNumForRead(String branchName);

    private KeyDistribution getKeyDistributionType(String keyDistributionName) {
	for (KeyDistribution kd : KeyDistribution.values()) {
	    if (kd.getName().equals(keyDistributionName)) {
		return kd;
	    }
	}
	throw new IllegalArgumentException("Invalid tuple load strategy!");
    }
}
