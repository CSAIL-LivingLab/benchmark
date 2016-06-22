package simpledb.versioned.benchmark.ycsb;

public interface Cleanupable {
    void cleanup() throws DBException;
}
