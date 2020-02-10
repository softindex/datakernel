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

package io.global.kv.stub;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.global.common.SignedData;
import io.global.kv.api.KvStorage;
import io.global.kv.api.RawKvItem;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class RuntimeKvStorageStub implements KvStorage {
	private final Map<ByteArrayWrapper, SignedData<RawKvItem>> storage = new ConcurrentHashMap<>();

	@Override
	public Promise<ChannelConsumer<SignedData<RawKvItem>>> upload() {
		return Promise.of(ChannelConsumer.ofConsumer(item -> {
			SignedData<RawKvItem> prev = storage.get(wrap(item.getValue().getKey()));
			if (prev == null || item.getValue().getTimestamp() >= prev.getValue().getTimestamp()) {
				storage.put(wrap(item.getValue().getKey()), item);
			}
		}));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawKvItem>>> download(long timestamp) {
		if (storage.values().isEmpty()){
			return Promise.of(null);
		}
		return Promise.of(ChannelSupplier.ofStream(storage.values()
				.stream()
				.filter(item -> item.getValue().getTimestamp() >= timestamp)));
	}

	@Override
	public Promise<ChannelConsumer<SignedData<byte[]>>> remove() {
		return Promise.of(ChannelConsumer.ofConsumer(key -> storage.remove(wrap(key.getValue()))));
	}

	@Override
	public Promise<SignedData<RawKvItem>> get(byte[] key) {
		return Promise.of(storage.get(wrap(key)));
	}

	@Override
	public Promise<Void> put(SignedData<RawKvItem> item) {
		storage.put(wrap(item.getValue().getKey()), item);
		return Promise.complete();
	}

	@Override
	public Promise<Void> remove(SignedData<byte[]> key) {
		storage.remove(wrap(key.getValue()));
		return Promise.complete();
	}

	private static ByteArrayWrapper wrap(byte[] data) {
		return new ByteArrayWrapper(data);
	}

	private static final class ByteArrayWrapper {
		private final byte[] data;

		private ByteArrayWrapper(byte[] data) {
			this.data = data;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof ByteArrayWrapper)) {
				return false;
			}
			return Arrays.equals(data, ((ByteArrayWrapper) other).data);
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(data);
		}
	}
}
