package io.global.common.discovery;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.UncheckedException;
import io.datakernel.util.Tuple2;
import io.datakernel.util.TypeT;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.SharedKeyStorage;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.Utils.arrayStartsWith;
import static io.global.common.BinaryDataFormats.REGISTRY;

public class RocksDbSharedKeyStorage implements SharedKeyStorage {
	private static final Logger logger = LoggerFactory.getLogger(RocksDbAnnouncementStorage.class);

	private static final StructuredCodec<PubKey> PUB_KEY_CODEC = REGISTRY.get(PubKey.class);
	private static final StructuredCodec<Tuple2<PubKey, Hash>> KEY_CODEC = REGISTRY.get(new TypeT<Tuple2<PubKey, Hash>>() {});
	private static final StructuredCodec<SignedData<SharedSimKey>> SHARED_SIM_KEY_CODEC = REGISTRY.get(new TypeT<SignedData<SharedSimKey>>() {});

	private final RocksDB db;
	private final WriteOptions writeOptions;

	@Nullable
	private Executor executor;

	private RocksDbSharedKeyStorage(RocksDB db, WriteOptions writeOptions) {
		this.db = db;
		this.writeOptions = writeOptions;
	}

	public static RocksDbSharedKeyStorage create(RocksDB db) {
		WriteOptions writeOptions = new WriteOptions().setSync(true);
		return new RocksDbSharedKeyStorage(db, writeOptions);
	}

	public static RocksDbSharedKeyStorage create(RocksDB db, WriteOptions writeOptions) {
		return new RocksDbSharedKeyStorage(db, writeOptions);
	}

	public RocksDbSharedKeyStorage withExecutor(Executor executor) {
		this.executor = executor;
		return this;
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
				.acceptEx(toLogger(logger, thisMethod(), receiver, signedSharedSimKey));
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
				.acceptEx(toLogger(logger, thisMethod(), receiver, hash));
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
				.acceptEx(toLogger(logger, thisMethod(), receiver));
	}
}
