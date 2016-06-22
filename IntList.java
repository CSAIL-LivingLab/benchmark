package simpledb.versioned.benchmark;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

import simpledb.IntField;

public class IntList implements Iterable<IntField> {

	protected static final int DEFAULT_INITIAL_SIZE = 1000000;

	protected int[] elements;
	protected int endPointer;
	protected BitSet activeIndex;
	protected int size;

	public IntList() {
		size = 0;
		endPointer = -1;
		activeIndex = new BitSet();
		elements = new int[DEFAULT_INITIAL_SIZE];
	}

	public void add(int item) {
		if (size == elements.length) {
			expand(2 * elements.length);
		}
		elements[++endPointer] = item;
		activeIndex.set(endPointer);
		size++;
	}

	public void remove(int index) {
		activeIndex.clear(index);
		size--;
	}

	public void clear() {
		size = 0;
		endPointer = -1;
		activeIndex.clear();
	}

	private void expand(int targetSize) {
		if (targetSize <= size) {
			throw new IllegalArgumentException("Invalid Size!");
		}
		elements = Arrays.copyOf(elements, targetSize);
	}

	public int size() {
		return size;
	}

	@Override
	public Iterator<IntField> iterator() {
		return new IntListIterator(elements, activeIndex);
	}

	@Override
	public void forEach(Consumer<? super IntField> action) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Spliterator<IntField> spliterator() {
		throw new UnsupportedOperationException();
	}
}
