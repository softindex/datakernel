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

package io.global.db;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.*;
import io.global.db.api.GlobalDbNode;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

import static io.global.db.util.BinaryDataFormats.REGISTRY;

public final class GlobalDbDriver {
	private static final Logger logger = LoggerFactory.getLogger(GlobalDbDriver.class);

	private static final StructuredCodec<DbItem> DB_ITEM_CODEC = REGISTRY.get(DbItem.class);

	private final GlobalDbNode node;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private GlobalDbDriver(GlobalDbNode node) {
		this.node = node;
	}

	public static GlobalDbDriver create(GlobalDbNode node) {
		return new GlobalDbDriver(node);
	}

	public Promise<ChannelConsumer<DbItem>> upload(KeyPair keys, String table, @Nullable SimKey simKey) {
		PrivKey privKey = keys.getPrivKey();
		return node.upload(keys.getPubKey(), table)
				.map(consumer ->
						consumer.map(item ->
								SignedData.sign(DB_ITEM_CODEC, DbItem.encrypt(item, simKey), privKey)));
	}

	public Promise<ChannelSupplier<DbItem>> download(PubKey space, String table, long timestamp, @Nullable SimKey simKey) {
		Hash simKeyHash = simKey != null ? Hash.sha1(simKey.getBytes()) : null;
		return node.download(space, table, timestamp)
				.map(supplier -> supplier
						.filter(signedItem -> {
							if (!signedItem.verify(space)) {
								logger.warn("received key-value pair with a signature that is not verified, skipping");
								return false;
							}
							DbItem value = signedItem.getValue();
							return !value.isRemoved() && Objects.equals(value.getSimKeyHash(), simKeyHash);
						})
						.map(signedDbItem -> DbItem.decrypt(signedDbItem.getValue(), simKey)));
	}

	public Promise<ChannelConsumer<byte[]>> remove(KeyPair keys, String table) {
		PrivKey privKey = keys.getPrivKey();
		return node.upload(keys.getPubKey(), table)
				.map(consumer -> consumer
						.map(key -> SignedData.sign(DB_ITEM_CODEC, DbItem.ofRemoved(key, now.currentTimeMillis()), privKey)));
	}

	public Promise<DbItem> get(PubKey space, String table, byte[] key, @Nullable SimKey simKey) {
		return node.get(space, table, key)
				.map(signedDbItem -> DbItem.decrypt(signedDbItem.getValue(), simKey));
	}

	public Promise<Void> put(KeyPair keys, String table, DbItem item, @Nullable SimKey simKey) {
		return ChannelSupplier.of(item).streamTo(ChannelConsumer.ofPromise(upload(keys, table, simKey)));
	}

	public Promise<Void> remove(KeyPair keys, String table, byte[] key) {
		return ChannelSupplier.of(key).streamTo(ChannelConsumer.ofPromise(remove(keys, table)));
	}

	public Promise<List<String>> list(PubKey space) {
		return node.list(space);
	}

	public GlobalDbAdapter adapt(PubKey owner) {
		return new GlobalDbAdapter(this, owner, null);
	}

	public GlobalDbAdapter adapt(PrivKey privKey) {
		return new GlobalDbAdapter(this, privKey.computePubKey(), privKey);
	}

	public GlobalDbAdapter adapt(KeyPair keys) {
		return new GlobalDbAdapter(this, keys.getPubKey(), keys.getPrivKey());
	}
}
