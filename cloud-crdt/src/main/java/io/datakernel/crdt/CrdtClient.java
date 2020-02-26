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

import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;

/**
 * Interface for various CRDT client implementations.
 * CRDT client can be seen as SortedMap where put operation is same as merge
 * with CRDT union as combiner.
 *
 * @param <K> crdt key type
 * @param <S> crdt state type.
 */
public interface CrdtClient<K extends Comparable<K>, S> {
	/**
	 * Returns a consumer of key-state pairs to be added/merged to the CRDT
	 * storage represented by this client.
	 *
	 * @return a promise of a stream consumer of key-state pairs.
	 */
	Promise<StreamConsumer<CrdtData<K, S>>> upload();

	/**
	 * Returns a supplier of all key-state pairs with <i>partial</i> states
	 * that are extracted using given revision.
	 * These pairs must be sorted by the key.
	 *
	 * @return a promise of a supplier of key-state pairs.
	 * @see CrdtOperator#extract
	 */
	Promise<StreamSupplier<CrdtData<K, S>>> download(long revision);

	/**
	 * Default download shortcut for zeroth revision - this will return a
	 * supplier of all key-state pairs with <i>full</i> states.
	 *
	 * @return a promise of a supplier of key-state pairs.
	 */
	default Promise<StreamSupplier<CrdtData<K, S>>> download() {
		return download(0);
	}

	/**
	 * Returns a consumer of keys to be removed from the CRDT storage.
	 * This operation is not persistent and not guaranteed.
	 *
	 * @return a promise of stream consumer of keys.
	 */
	Promise<StreamConsumer<K>> remove();

	/**
	 * Marker that this client is functional (server is up, there are enough
	 * nodes in cluster etc.)
	 *
	 * @return a promise that succeeds if this client is up and correct.
	 */
	Promise<Void> ping();

	default Promise<Void> fetch(CrdtClient<K, S> other, long revision) {
		return other.download(revision)
				.then(supplier -> supplier.streamTo(upload()));
	}

	default Promise<Void> fetchAll(CrdtClient<K, S> other) {
		return fetch(other, 0);
	}
}
