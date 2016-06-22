package simpledb.versioned.benchmark;

import java.util.BitSet;
import java.util.Iterator;
import java.util.function.Consumer;

import simpledb.IntField;
import simpledb.MutableIntField;

public class IntListIterator implements Iterator<IntField> {
	final int[] elements;
	final MutableIntField out;
	BitSet activeIndex;
	int position;

	public IntListIterator(int[] elements, BitSet activeIndex) {
		this.elements = elements;
		this.activeIndex = activeIndex;
		position = -1;
		out = new MutableIntField(-1);
		setNext();
	}

	protected void setNext() {
		position = activeIndex.nextSetBit(position + 1);
	}

	@Override
	public boolean hasNext() {
		return position != -1;
	}

	@Override
	public IntField next() {
		int element = elements[position];
		out.setValue(element);
		setNext();
		return out;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void forEachRemaining(Consumer<? super IntField> action) {
		throw new UnsupportedOperationException();
	}

}