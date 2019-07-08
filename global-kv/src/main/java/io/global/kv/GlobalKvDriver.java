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
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.time.CurrentTimeProvider;
import io.global.common.*;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.KvItem;
import io.global.kv.api.RawKvItem;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.common.Hash.sha1;
import static io.global.kv.util.BinaryDataFormats.RAW_KV_ITEM_CODEC;

public final class GlobalKvDriver<K, V> {
	private static final Logger logger = LoggerFactory.getLogger(GlobalKvDriver.class);

	private final GlobalKvNode node;
	private final StructuredCodec<K> keyCodec;
	private final StructuredCodec<V> valueCodec;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private GlobalKvDriver(GlobalKvNode node, StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
		this.node = node;
		this.keyCodec = keyCodec;
		this.valueCodec = valueCodec;
	}

	public static <K, V> GlobalKvDriver<K, V> create(GlobalKvNode node, StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
		return new GlobalKvDriver<>(node, keyCodec, valueCodec);
	}

	private static byte[] crypt(byte[] key, byte[] bytes, @Nullable SimKey simKey) {
		if (simKey == null) {
			return bytes;
		}
		// TODO anton: replace with inplace when tests will use file storage instead of runtime stubs
		byte[] copy = new byte[bytes.length];
		System.arraycopy(bytes, 0, copy, 0, bytes.length);
		CTRAESCipher.create(simKey.getAesKey(), CryptoUtils.nonceFromBytes(key)).apply(copy);
		return copy;
	}

	@NotNull
	private SignedData<RawKvItem> encrypt(KvItem<K, V> item, PrivKey privKey, @Nullable SimKey simKey) {
		byte[] key = encodeAsArray(keyCodec, item.getKey());
		byte[] value = crypt(key, encodeAsArray(valueCodec, item.getValue()), simKey);
		RawKvItem raw = new RawKvItem(key, value, item.getTimestamp(), simKey != null ? sha1(simKey.getBytes()) : null);
		return SignedData.sign(RAW_KV_ITEM_CODEC, raw, privKey);
	}

	private Promise<@Nullable KvItem<K, V>> decrypt(@Nullable SignedData<RawKvItem> signedRawKvItem, @Nullable SimKey simKey) {
		try {
			if (signedRawKvItem == null) {
				return Promise.of(null);
			}
			RawKvItem raw = signedRawKvItem.getValue();
			byte[] key = raw.getKey();
			byte[] value = simKey != null ? crypt(key, raw.getValue(), simKey) : raw.getValue();
			return Promise.of(new KvItem<>(raw.getTimestamp(), BinaryUtils.decode(keyCodec, key), BinaryUtils.decode(valueCodec, value)));
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	}

	public Promise<ChannelConsumer<KvItem<K, V>>> upload(KeyPair keys, String table, @Nullable SimKey simKey) {
		PrivKey privKey = keys.getPrivKey();
		return node.upload(keys.getPubKey(), table)
				.map(consumer -> consumer.map(item -> encrypt(item, privKey, simKey)));
	}

	public Promise<ChannelSupplier<KvItem<K, V>>> download(PubKey space, String table, long timestamp, @Nullable SimKey simKey) {
		Hash simKeyHash = simKey != null ? sha1(simKey.getBytes()) : null;
		return node.download(space, table, timestamp)
				.map(supplier -> supplier
						.filter(signedItem -> {
							if (!signedItem.verify(space)) {
								logger.warn("received key-value pair with a signature that is not verified, skipping");
								return false;
							}
							RawKvItem raw = signedItem.getValue();
							return !raw.isRemoved() && Objects.equals(raw.getSimKeyHash(), simKeyHash);
						})
						.mapAsync(signedRawKvItem -> decrypt(signedRawKvItem, simKey)));
	}

	public Promise<ChannelConsumer<byte[]>> remove(KeyPair keys, String table) {
		PrivKey privKey = keys.getPrivKey();
		return node.upload(keys.getPubKey(), table)
				.map(consumer -> consumer
						.map(key -> SignedData.sign(RAW_KV_ITEM_CODEC, RawKvItem.ofRemoved(key, now.currentTimeMillis()), privKey)));
	}

	public Promise<@Nullable KvItem<K, V>> get(PubKey space, String table, K key, @Nullable SimKey simKey) {
		return node.get(space, table, encodeAsArray(keyCodec, key))
				.then(signedRawKvItem -> decrypt(signedRawKvItem, simKey));
	}

	public Promise<Void> put(KeyPair keys, String table, KvItem<K, V> item, @Nullable SimKey simKey) {
		return node.put(keys.getPubKey(), table, encrypt(item, keys.getPrivKey(), simKey));
	}

	public Promise<Void> remove(KeyPair keys, String table, byte[] key) {
		return node.put(keys.getPubKey(), table, SignedData.sign(RAW_KV_ITEM_CODEC, RawKvItem.ofRemoved(key, now.currentTimeMillis()), keys.getPrivKey()));
	}

	public Promise<Set<String>> list(PubKey space) {
		return node.list(space);
	}

	public GlobalKvAdapter<K, V> adapt(PubKey owner) {
		return new GlobalKvAdapter<>(this, owner, null);
	}

	public GlobalKvAdapter<K, V> adapt(PrivKey privKey) {
		return new GlobalKvAdapter<>(this, privKey.computePubKey(), privKey);
	}

	public GlobalKvAdapter<K, V> adapt(KeyPair keys) {
		return new GlobalKvAdapter<>(this, keys.getPubKey(), keys.getPrivKey());
	}

	public GlobalKvNode getNode() {
		return node;
	}

	public StructuredCodec<K> getKeyCodec() {
		return keyCodec;
	}

	public StructuredCodec<V> getValueCodec() {
		return valueCodec;
	}
}
