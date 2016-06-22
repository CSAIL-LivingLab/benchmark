package simpledb.versioned.benchmark;

import java.util.Arrays;
import java.util.BitSet;

import simpledb.versioned.benchmark.ycsb.generator.RangeIntegerGenerator;
import simpledb.versioned.benchmark.ycsb.generator.UniformIntegerGenerator;

public class LinkedIntSetDistribution extends IntList {

    RangeIntegerGenerator generator;
    int[] elementToIndexMap;
    BitSet elementSet;

    public LinkedIntSetDistribution(RangeIntegerGenerator generator) {
	this.generator = generator;
	elementSet = new BitSet();
	elementToIndexMap = new int[IntList.DEFAULT_INITIAL_SIZE];
	Arrays.fill(elementToIndexMap, -1);
    }

    public LinkedIntSetDistribution() {
	this(new UniformIntegerGenerator(0, 0));
    }

    @Override
    public void add(int key) {
	if (!elementSet.get(key)) {
	    int mapSize = elementToIndexMap.length;
	    if (key >= elementToIndexMap.length) {
		expandMap(Math.max(key + 1, 2 * mapSize));
	    }
	    super.add(key);
	    elementToIndexMap[key] = endPointer;
	    elementSet.set(key);
	    generator.updateRange(0, endPointer);
	}
    }

    private void expandMap(int targetSize) {
	if (targetSize <= elementToIndexMap.length) {
	    throw new IllegalArgumentException("Invalid Size!");
	}
	elementToIndexMap = Arrays.copyOf(elementToIndexMap, targetSize);
    }

    @Override
    public void remove(int key) {
	if (elementSet.get(key)) {
	    int index = elementToIndexMap[key];
	    super.remove(index);
	    elementSet.clear(key);
	}
    }

    public int getRandomKey() {
	if (size == 0) {
	    throw new IllegalStateException("No keys!");
	}
	while (true) {
	    int index = generator.nextInt();
	    if (activeIndex.get(index)) {
		return elements[index];
	    }
	}
    }

    public void getRandomKeys(IntList outList, int numKeys) {
	if (size == 0) {
	    throw new IllegalStateException("No keys!");
	}
	while (numKeys > 0) {
	    while (true) {
		int index = generator.nextInt();
		if (activeIndex.get(index)) {
		    outList.add(elements[index]);
		    break;
		}
	    }
	    numKeys--;
	}
    }

    public RangeIntegerGenerator getDistribution() {
	return generator;
    }

    public void addAll(LinkedIntSetDistribution keys) {
	BitSet bs = keys.activeIndex;
	for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
	    add(keys.elements[i]);
	}
    }

    public void removeAll(IntList existingKeysToRemove) {
	BitSet bs = existingKeysToRemove.activeIndex;
	for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
	    remove(existingKeysToRemove.elements[i]);
	}
    }

    public void addAll(IntList newKeys) {
	BitSet bs = newKeys.activeIndex;
	for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
	    add(newKeys.elements[i]);
	}
    }

    public boolean contains(int val) {
	return elementSet.get(val);
    }

}
