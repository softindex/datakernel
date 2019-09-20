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

package io.global.kv;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SimKey;
import io.global.kv.api.KvClient;
import io.global.kv.api.KvItem;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public final class GlobalKvAdapter<K, V> implements KvClient<K, V> {
	public static final StacklessException UPK_UPLOAD = new StacklessException(GlobalKvAdapter.class, "Trying to upload to public key without knowing it's private key");
	public static final StacklessException UPK_DELETE = new StacklessException(GlobalKvAdapter.class, "Trying to delete file at public key without knowing it's private key");

	private final GlobalKvDriver<K, V> driver;
	private final PubKey space;

	@Nullable
	private final PrivKey privKey;

	@Nullable
	private SimKey currentSimKey = null;

	public GlobalKvAdapter(GlobalKvDriver<K, V> driver, PubKey space, @Nullable PrivKey privKey) {
		this.driver = driver;
		this.space = space;
		this.privKey = privKey;
	}

	@Nullable
	public SimKey getCurrentSimKey() {
		return currentSimKey;
	}

	public void setCurrentSimKey(@Nullable SimKey currentSimKey) {
		this.currentSimKey = currentSimKey;
	}

	@Override
	public Promise<ChannelConsumer<KvItem<K, V>>> upload(String table) {
		return privKey != null ?
				driver.upload(new KeyPair(privKey, space), table, currentSimKey) :
				Promise.ofException(UPK_UPLOAD);
	}

	@Override
	public Promise<ChannelSupplier<KvItem<K, V>>> download(String table, long timestamp) {
		return driver.download(space, table, timestamp, currentSimKey);
	}

	@Override
	public Promise<ChannelConsumer<K>> remove(String table) {
		return privKey != null ?
				driver.remove(new KeyPair(privKey, space), table) :
				Promise.ofException(UPK_DELETE);
	}

	@Override
	public Promise<KvItem<K, V>> get(String table, K key) {
		return driver.get(space, table, key, currentSimKey);
	}

	@Override
	public Promise<Set<String>> list() {
		return driver.list(space);
	}
}
