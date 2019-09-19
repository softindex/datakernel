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

package io.global.kv.api;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;

import java.util.Set;

public interface KvClient<K, V> {
	Promise<ChannelConsumer<KvItem<K, V>>> upload(String table);

	Promise<ChannelSupplier<KvItem<K, V>>> download(String table, long timestamp);

	default Promise<ChannelSupplier<KvItem<K, V>>> download(String table) {
		return download(table, 0);
	}

	Promise<ChannelConsumer<byte[]>> remove(String table);

	Promise<KvItem<K, V>> get(String table, K key);

	default Promise<Void> put(String table, KvItem<K, V> item) {
		return ChannelSupplier.of(item).streamTo(ChannelConsumer.ofPromise(upload(table)));
	}

	default Promise<Void> remove(String table, byte[] key) {
		return ChannelSupplier.of(key).streamTo(ChannelConsumer.ofPromise(remove(table)));
	}

	Promise<Set<String>> list();
}
