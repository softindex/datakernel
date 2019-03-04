package io.global.common.discovery;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.UncheckedException;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.util.concurrent.Executor;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.common.BinaryDataFormats.REGISTRY;

public class RocksDbAnnouncementStorage implements AnnouncementStorage {
	private static final StructuredCodec<PubKey> PUB_KEY_CODEC = REGISTRY.get(PubKey.class);
	private static final StructuredCodec<SignedData<AnnounceData>> ANNOUNCEMENT_CODEC = REGISTRY.get(new TypeT<SignedData<AnnounceData>>() {});

	private final RocksDB db;
	private final WriteOptions writeOptions;

	@Nullable
	private Executor executor;

	private RocksDbAnnouncementStorage(RocksDB db, WriteOptions writeOptions) {
		this.db = db;
		this.writeOptions = writeOptions;
	}

	public static RocksDbAnnouncementStorage create(RocksDB db) {
		WriteOptions writeOptions = new WriteOptions().setSync(true);
		return new RocksDbAnnouncementStorage(db, writeOptions);
	}

	public static RocksDbAnnouncementStorage create(RocksDB db, WriteOptions writeOptions) {
		return new RocksDbAnnouncementStorage(db, writeOptions);
	}

	public RocksDbAnnouncementStorage withExecutor(Executor executor) {
		this.executor = executor;
		return this;
	}

	@Override
	public Promise<Void> store(PubKey space, SignedData<AnnounceData> announceData) {
		return Promise.ofBlockingRunnable(executor, () -> {
			try {
				byte[] keyBytes = encodeAsArray(PUB_KEY_CODEC, space);
				byte[] valueBytes = encodeAsArray(ANNOUNCEMENT_CODEC, announceData);

				db.put(writeOptions, keyBytes, valueBytes);
			} catch (RocksDBException e) {
				throw new UncheckedException(e);
			}
		});
	}

	@Override
	public Promise<SignedData<AnnounceData>> load(PubKey space) {
		return Promise.ofBlockingCallable(executor, () -> {
			byte[] keyBytes = encodeAsArray(PUB_KEY_CODEC, space);
			byte[] valueBytes = db.get(keyBytes);

			if (valueBytes == null) {
				throw new UncheckedException(NO_ANNOUNCEMENT);
			} else {
				return decode(ANNOUNCEMENT_CODEC, valueBytes);
			}
		});
	}
}
