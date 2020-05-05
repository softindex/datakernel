package io.datakernel.crdt.function;

import io.datakernel.crdt.primitives.CrdtType;
import org.jetbrains.annotations.Nullable;

public interface CrdtFunction<S> {

	/**
	 * This method should combine two given CRDT values together,
	 * not violating any of the CRDT conditions.
	 */
	S merge(S first, S second);

	/**
	 * Extract partial CRDT state from given state, which contains only the
	 * changes to it since given timestamp.
	 * <p>
	 * If there were no changes, then this method <b>must</b> return <code>null</code>.
	 * <p>
	 * Suppose we have some CRDT value 'old', which is the state of something
	 * either exactly at or after the timestamp.
	 * <p>
	 * This method should create a CRDT value that, when combined with 'old',
	 * gives you 'state', which is 'old' updated to current time.
	 * <p>
	 * Basically this is almost like taking a CRDT diff betweend 'old' and 'state'.
	 * <p>
	 * It can be a huge optimization e.g. for big CRDT maps:
	 * <p>
	 * The whole map state could contain thousands of key-value pairs,
	 * and instead of combining 'old' with 'state' (which with CRDT would achieve the most complete map)
	 * that requires serializing, transfering and/or storing the whole 'state',
	 * one could extract only the entries that are changed between 'old' and 'state',
	 * serialize and transfer only those and then CRDT-combine those with 'old', achieving the
	 * same result with much less resources spent.
	 */
	@Nullable
	S extract(S state, long timestamp);

	static <S extends CrdtType<S>> CrdtFunction<S> ofCrdtType() {
		return new CrdtFunction<S>() {
			@Override
			public S merge(S first, S second) {
				return first.merge(second);
			}

			@Nullable
			@Override
			public S extract(S state, long timestamp) {
				return state.extract(timestamp);
			}
		};
	}
}
