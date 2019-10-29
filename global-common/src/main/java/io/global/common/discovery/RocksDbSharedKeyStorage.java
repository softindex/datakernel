package io.global.common.discovery;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.promise.Promise;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.SharedKeyStorage;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static io.datakernel.async.util.LogUtils.thisMethod;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.util.Utils.arrayStartsWith;

public class RocksDbSharedKeyStorage implements SharedKeyStorage {
	private static final Logger logger = LoggerFactory.getLogger(RocksDbAnnouncementStorage.class);

	private static final StructuredCodec<PubKey> PUB_KEY_CODEC = REGISTRY.get(PubKey.class);
	private static final StructuredCodec<Tuple2<PubKey, Hash>> KEY_CODEC = REGISTRY.get(new TypeT<Tuple2<PubKey, Hash>>() {});
	private static final StructuredCodec<SignedData<SharedSimKey>> SHARED_SIM_KEY_CODEC = REGISTRY.get(new TypeT<SignedData<SharedSimKey>>() {});

	private final RocksDB db;
	private final WriteOptions writeOptions;

	@NotNull
	private final Executor executor;

	private RocksDbSharedKeyStorage(@NotNull Executor executor, RocksDB db, WriteOptions writeOptions) {
		this.db = db;
		this.writeOptions = writeOptions;
		this.executor = executor;
	}

	public static RocksDbSharedKeyStorage create(@NotNull Executor executor, RocksDB db) {
		WriteOptions writeOptions = new WriteOptions().setSync(true);
		return new RocksDbSharedKeyStorage(executor, db, writeOptions);
	}

	public static RocksDbSharedKeyStorage create(@NotNull Executor executor, RocksDB db, WriteOptions writeOptions) {
		return new RocksDbSharedKeyStorage(executor, db, writeOptions);
	}

	@Override
	public Promise<Void> store(PubKey receiver, SignedData<SharedSimKey> signedSharedSimKey) {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					try {
						Hash hash = signedSharedSimKey.getValue().getHash();
						byte[] keyBytes = encodeAsArray(KEY_CODEC, new Tuple2<>(receiver, hash));
						byte[] valueBytes = encodeAsArray(SHARED_SIM_KEY_CODEC, signedSharedSimKey);

						db.put(writeOptions, keyBytes, valueBytes);
					} catch (RocksDBException e) {
						throw new UncheckedException(e);
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), receiver, signedSharedSimKey));
	}

	@Override
	public Promise<SignedData<SharedSimKey>> load(PubKey receiver, Hash hash) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					byte[] keyBytes = encodeAsArray(KEY_CODEC, new Tuple2<>(receiver, hash));
					byte[] valueBytes = db.get(keyBytes);

					if (valueBytes == null) {
						return null;
					} else {
						return decode(SHARED_SIM_KEY_CODEC, valueBytes);
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), receiver, hash));
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> loadAll(PubKey receiver) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					byte[] prefixBytes = encodeAsArray(PUB_KEY_CODEC, receiver);
					List<SignedData<SharedSimKey>> result = new ArrayList<>();
					try (RocksIterator iterator = db.newIterator()) {
						for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next()) {
							byte[] keyBytes = iterator.key();
							if (!arrayStartsWith(keyBytes, prefixBytes)) {
								break;
							}

							result.add(decode(SHARED_SIM_KEY_CODEC, iterator.value()));
						}
						return result;
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), receiver));
	}
}
