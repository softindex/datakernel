/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.kv;

import io.datakernel.common.ref.Ref;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public interface KvClient<K, V> {
	Promise<StreamConsumer<KvItem<K, V>>> upload(String table);

	Promise<StreamSupplier<KvItem<K, V>>> download(String table, long timestamp);

	default Promise<StreamSupplier<KvItem<K, V>>> download(String table) {
		return download(table, 0);
	}

	Promise<StreamConsumer<K>> remove(String table);

	default Promise<@Nullable KvItem<K, V>> get(String table, K key) {
		Ref<KvItem<K, V>> result = new Ref<>();
		return download(table)
				.then(supplier -> supplier.streamTo(StreamConsumer.of(item -> {
					if (item.getKey().equals(key)) {
						result.set(item);
						supplier.close();
					}
				})))
				.map($ -> result.get());
	}

	default Promise<Void> put(String table, KvItem<K, V> item) {
		return StreamSupplier.of(item).streamTo(StreamConsumer.ofPromise(upload(table)));
	}

	default Promise<Void> remove(String table, K key) {
		return StreamSupplier.of(key).streamTo(StreamConsumer.ofPromise(remove(table)));
	}

	Promise<Set<String>> list();
}
