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
import io.datakernel.exception.StacklessException;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.Hash;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.db.api.DbClient;
import io.global.db.api.GlobalDbNode;
import io.global.db.api.TableID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.global.db.util.BinaryDataFormats.REGISTRY;

public final class GlobalDbGateway implements DbClient {
	private static final Logger logger = LoggerFactory.getLogger(GlobalDbGateway.class);

	private static final StacklessException DB_ITEM_SIG = new StacklessException(GlobalDbGateway.class, "Received key-value pair signature is not verified");

	private static final StructuredCodec<DbItem> DB_ITEM_CODEC = REGISTRY.get(DbItem.class);

	private final GlobalDbDriver driver;
	private final GlobalDbNode node;
	private final PubKey owner;
	private final PrivKey privKey;

	CurrentTimeProvider currentTimeProvider = CurrentTimeProvider.ofSystem();

	public GlobalDbGateway(GlobalDbDriver driver, GlobalDbNode node, PubKey owner, PrivKey privKey) {
		this.driver = driver;
		this.node = node;
		this.owner = owner;
		this.privKey = privKey;
	}

	private Promise<DbItem> decrypt(DbItem item) {
		Hash simKeyHash = item.getSimKeyHash();
		return simKeyHash != null ?
				driver.getPrivateKeyStorage()
						.getKey(owner, simKeyHash)
						.map(simKey -> simKey != null ? DbItem.decrypt(item, simKey) : item) :
				Promise.of(item);
	}

	@Override
	public Promise<ChannelConsumer<DbItem>> upload(String table) {
		return node.upload(TableID.of(owner, table))
				.map(consumer ->
						consumer.map(item ->
								SignedData.sign(DB_ITEM_CODEC, DbItem.encrypt(item, driver.getPrivateKeyStorage().getCurrentSimKey()), privKey)));
	}

	@Override
	public Promise<ChannelSupplier<DbItem>> download(String table, long timestamp) {
		return node.download(TableID.of(owner, table), timestamp)
				.map(supplier ->
						(supplier
								.filter(signedItem -> {
									if (!signedItem.verify(owner)) {
										logger.warn("received key-value pair with a signature that is not verified, skipping");
										return false;
									}

									// also skip tombstones
									return !signedItem.getValue().isRemoved();
								})
								.map(SignedData::getValue)
								.mapAsync(this::decrypt)));
	}

	@Override
	public Promise<ChannelConsumer<byte[]>> remove(String table) {
		return node.upload(TableID.of(owner, table))
				.map(consumer ->
						consumer.map(key -> SignedData.sign(DB_ITEM_CODEC, DbItem.ofRemoved(key, currentTimeProvider.currentTimeMillis()), privKey)));
	}

	@Override
	public Promise<DbItem> get(String table, byte[] key) {
		return node.get(TableID.of(owner, table), key)
				.then(signedDbItem -> {
					if (!signedDbItem.verify(owner)) {
						return Promise.ofException(DB_ITEM_SIG);
					}
					DbItem item = signedDbItem.getValue();
					return decrypt(item);
				});
	}

	@Override
	public Promise<List<String>> list() {
		return node.list(owner);
	}
}
