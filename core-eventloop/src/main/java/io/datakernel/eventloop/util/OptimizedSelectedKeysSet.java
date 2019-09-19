package io.datakernel.eventloop.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.channels.SelectionKey;
import java.util.AbstractSet;
import java.util.Iterator;

/**
 * The class is used to avoid the overhead in {@link sun.nio.ch.SelectorImpl}
 * This implementation is set instead of {@link java.util.HashSet}
 * It allows to avoid redundant work of GC
 * Should use it as a simple array instead of a set
 */
public final class OptimizedSelectedKeysSet extends AbstractSet<SelectionKey> {
	private static final int INITIAL_SIZE = 1 << 4;
	private int size;
	private SelectionKey[] selectionKeys;

	public OptimizedSelectedKeysSet() {
		selectionKeys = new SelectionKey[INITIAL_SIZE];
	}

	public OptimizedSelectedKeysSet(int initialSize) {
		selectionKeys = new SelectionKey[initialSize];
	}

	@Override
	public boolean add(SelectionKey selectionKey) {
		ensureCapacity();
		selectionKeys[size++] = selectionKey;
		return true;
	}

	/**
	 * Multiply the size of array twice
	 */
	private void ensureCapacity() {
		if (size < selectionKeys.length) {
			return;
		}
		SelectionKey[] newArray = new SelectionKey[selectionKeys.length * 2];
		System.arraycopy(selectionKeys, 0, newArray, 0, size);
		selectionKeys = newArray;
	}

	/**
	 * @param index the pointer to the Selection key from the array,
	 *              must be in range of {@param size}
	 * @return the {@link SelectionKey} from the array by index
	 */
	@Nullable
	public SelectionKey get(int index) {
		if (index >= 0 && index < size) {
			return selectionKeys[index];
		}
		return null;
	}

	@NotNull
	@Override
	public Iterator<SelectionKey> iterator() {
		return new Iterator<SelectionKey>() {
			int step;

			@Override
			public boolean hasNext() {
				return step < size;
			}

			@Override
			public SelectionKey next() {
				return selectionKeys[step++];
			}
		};
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public void clear() {
		size = 0;
	}
}
