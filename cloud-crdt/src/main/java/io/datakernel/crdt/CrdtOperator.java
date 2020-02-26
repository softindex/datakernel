/*
 * Copyright (C) 2015-2020 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.crdt;

import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

public interface CrdtOperator<S> {

	/**
	 * This method must combine two CRDT values together in commutative,
	 * associative, and idempotent manner as by CvRDT definition.
	 */
	S merge(S first, S second);

	/**
	 * Extract partial CRDT state from the given state, which contains only the
	 * changes made to it after the given revision.
	 * <p>
	 * Suppose we have a CRDT value <i>old</i> which is equal on a two
	 * different machines.
	 * On a second machine, value got updated to <i>fresh</i> by merging with
	 * some other CRDT value(s).
	 * With conventional CvRDT, we would have to transfer the whole
	 * <i>fresh</i> state back to the first machine to synchronize the states
	 * and then merge it with <i>old</i> state to get the <i>fresh</i> state on
	 * it.
	 * However, with this optimization, we introduce the concept of revision,
	 * that is an identifier of CRDT state changes.
	 * In this example, let the <i>old</i> state have a revision of 1.
	 * When the state changes, its revision should be increased by some value,
	 * and then this function might extract the "CRDT difference" between
	 * <i>old</i> (identified by the revision of 1) and <i>fresh</i> (which is
	 * the current one) states - this can be called a <i>diff</i> state.
	 * The <i>diff</i> state combined with <i>old</i> state should then yield
	 * the <i>fresh</i> state, meaning we only have to transfer that
	 * <i>diff</i> state, which should take up less or equal memory to that of
	 * whole <i>fresh</i> state.
	 * <p>
	 * It can be a huge optimization e.g. for CRDT sets.
	 * <p>
	 * If there were no changes, then this method may return the
	 * <i>identity</i> state of the type {@link S} if it has one, or null if
	 * not.
	 * <p>
	 * This optimization may be unsupported in some cases or simply not
	 * implemented, in that case this method should simply return the original
	 * state passed to it (which makes perfect sense).
	 */
	@Nullable
	default S extract(S state, long revision) {
		return state;
	}

	/**
	 * Similar to how {@link Comparator} works with
	 * {@link Comparable} types, this is a universal CRDT function for any
	 * {@link Crdt} type that forwards the calls to its implementation.
	 */
	static <S extends Crdt<S>> CrdtOperator<S> ofCrdtType() {
		return new CrdtOperator<S>() {
			@Override
			public S merge(S first, S second) {
				return first.merge(second);
			}

			@Nullable
			@Override
			public S extract(S state, long revision) {
				return state.extract(revision);
			}
		};
	}
}
