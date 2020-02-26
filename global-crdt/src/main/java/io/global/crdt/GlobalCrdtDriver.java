package io.global.crdt;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.common.parse.ParseException;
import io.datakernel.crdt.CrdtData;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamFilter;
import io.datakernel.datastream.processor.StreamMapper;
import io.datakernel.promise.Promise;
import io.global.common.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.crdt.BinaryDataFormats.BYTES_CODEC;
import static io.global.crdt.BinaryDataFormats.RAW_CRDT_DATA_CODEC;

public final class GlobalCrdtDriver<K extends Comparable<K>, V> {
	private static final Logger logger = LoggerFactory.getLogger(GlobalCrdtDriver.class);

	private final GlobalCrdtNode node;
	private final StructuredCodec<K> keyCodec;
	private final StructuredCodec<V> stateCodec;

	private GlobalCrdtDriver(GlobalCrdtNode node, StructuredCodec<K> keyCodec, StructuredCodec<V> stateCodec) {
		this.node = node;
		this.keyCodec = keyCodec;
		this.stateCodec = stateCodec;
	}

	public static <K extends Comparable<K>, V> GlobalCrdtDriver<K, V> create(GlobalCrdtNode node, StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
		return new GlobalCrdtDriver<>(node, keyCodec, valueCodec);
	}

	private static byte[] crypt(byte[] key, byte[] bytes, @Nullable SimKey simKey) {
		if (simKey == null) {
			return bytes;
		}
		byte[] copy = new byte[bytes.length];
		System.arraycopy(bytes, 0, copy, 0, bytes.length);
		CTRAESCipher.create(simKey.getAesKey(), CryptoUtils.nonceFromBytes(key)).apply(copy);
		return copy;
	}

	@NotNull
	private SignedData<RawCrdtData> encrypt(CrdtData<K, V> item, PrivKey privKey, @Nullable SimKey simKey) {
		byte[] key = encodeAsArray(keyCodec, item.getKey());
		byte[] value = crypt(key, encodeAsArray(stateCodec, item.getState()), simKey);
		RawCrdtData raw = RawCrdtData.parse(key, value, simKey != null ? Hash.sha1(simKey.getBytes()) : null);
		return SignedData.sign(RAW_CRDT_DATA_CODEC, raw, privKey);
	}

	@Nullable
	private CrdtData<K, V> decrypt(@Nullable SignedData<RawCrdtData> signedRawCrdtData, @Nullable SimKey simKey) throws ParseException {
		if (signedRawCrdtData == null) {
			return null;
		}
		RawCrdtData raw = signedRawCrdtData.getValue();
		byte[] key = raw.getKey();
		byte[] value = simKey != null ? crypt(key, raw.getValue(), simKey) : raw.getValue();
		return new CrdtData<>(BinaryUtils.decode(keyCodec, key), BinaryUtils.decode(stateCodec, value));
	}

	public Promise<StreamConsumer<CrdtData<K, V>>> upload(KeyPair keys, String repo, @Nullable SimKey simKey) {
		PrivKey privKey = keys.getPrivKey();
		return node.upload(keys.getPubKey(), repo)
				.map(consumer -> consumer
						.transformWith(StreamMapper.create(item -> encrypt(item, privKey, simKey))));
	}

	public Promise<StreamSupplier<CrdtData<K, V>>> download(PubKey space, String table, long timestamp, @Nullable SimKey simKey) {
		Hash simKeyHash = simKey != null ? Hash.sha1(simKey.getBytes()) : null;
		return node.download(space, table, timestamp)
				.map(supplier -> supplier
						.transformWith(StreamFilter.create(signedItem -> {
							if (signedItem.verify(space)) {
								return Objects.equals(signedItem.getValue().getSimKeyHash(), simKeyHash);
							}
							logger.warn("received key-value pair with a signature that is not verified, skipping");
							return false;
						}))
						.transformWith(StreamMapper.create(signedRawCrdtData -> {
							try {
								return decrypt(signedRawCrdtData, simKey);
							} catch (ParseException e) {
								supplier.close(e);
								return null;
							}
						})));
	}

	public Promise<StreamConsumer<K>> remove(KeyPair keys, String table) {
		PrivKey privKey = keys.getPrivKey();
		return node.remove(keys.getPubKey(), table)
				.map(consumer -> consumer
						.transformWith(StreamMapper.create(key -> SignedData.sign(BYTES_CODEC, encodeAsArray(keyCodec, key), privKey))));
	}

	public Promise<Set<String>> list(PubKey space) {
		return node.list(space);
	}

	public GlobalCrdtAdapter<K, V> adapt(PubKey owner, String repo) {
		return new GlobalCrdtAdapter<>(this, owner, repo, null);
	}

	public GlobalCrdtAdapter<K, V> adapt(PrivKey privKey, String repo) {
		return new GlobalCrdtAdapter<>(this, privKey.computePubKey(), repo, privKey);
	}

	public GlobalCrdtAdapter<K, V> adapt(KeyPair keys, String repo) {
		return new GlobalCrdtAdapter<>(this, keys.getPubKey(), repo, keys.getPrivKey());
	}

	public GlobalCrdtNode getNode() {
		return node;
	}

	public StructuredCodec<K> getKeyCodec() {
		return keyCodec;
	}

	public StructuredCodec<V> getStateCodec() {
		return stateCodec;
	}
}
