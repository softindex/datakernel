package io.global.kv;

import io.datakernel.async.service.EventloopService;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.common.PubKey;
import io.global.kv.api.KvStorage;
import io.global.kv.api.StorageFactory;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static io.datakernel.async.util.LogUtils.Level.*;
import static io.datakernel.async.util.LogUtils.thisMethod;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.global.kv.util.BinaryDataFormats.REGISTRY;

public final class RocksDbStorageFactory implements EventloopService, StorageFactory {
	private static final Logger logger = LoggerFactory.getLogger(RocksDbStorageFactory.class);

	private static final StructuredCodec<Tuple2<PubKey, String>> DESCRIPTOR_CODEC = REGISTRY.get(new TypeT<Tuple2<PubKey, String>>() {});

	private final Eventloop eventloop;
	private final Executor executor;
	private final String rocksDbPath;
	private final Map<Tuple2<PubKey, String>, Promise<RocksDbKvStorage>> handlesMap = new HashMap<>();

	private RocksDB db;

	private RocksDbStorageFactory(Eventloop eventloop, Executor executor, String rocksDbPath) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.rocksDbPath = rocksDbPath;
	}

	public static RocksDbStorageFactory create(Eventloop eventloop, Executor executor, String rocksDbPath) {
		return new RocksDbStorageFactory(eventloop, executor, rocksDbPath);
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<Void> start() {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
					try (Options options = new Options()) {
						RocksDB.listColumnFamilies(options, rocksDbPath).stream()
								.map(ColumnFamilyDescriptor::new)
								.forEach(descriptors::add);
					}

					if (descriptors.isEmpty()) {
						logger.info("No column families found, initiating with default column family");
						descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
					}

					ArrayList<ColumnFamilyHandle> handles = new ArrayList<>();
					try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true)) {
						db = RocksDB.open(dbOptions, rocksDbPath, descriptors, handles);
					}

					// skipping default column family
					for (int i = 1; i < descriptors.size(); i++) {
						ColumnFamilyDescriptor columnFamilyDescriptor = descriptors.get(i);
						Tuple2<PubKey, String> descriptor = decode(DESCRIPTOR_CODEC, columnFamilyDescriptor.getName());
						handlesMap.put(descriptor, Promise.of(new RocksDbKvStorage(executor, db, handles.get(i))));
					}
				})
				.whenComplete(toLogger(logger, TRACE, TRACE, ERROR, thisMethod()));
	}

	@Override
	public @NotNull Promise<Void> stop() {
		return Promises.toList(handlesMap.values().stream()
				.map(storagePromise -> storagePromise
						.then(storage -> storage.flush()
								.map($ -> storage.getHandle()))))
				.then(handles -> Promise.ofBlockingRunnable(executor, () -> {
					handles.forEach(AbstractImmutableNativeReference::close);
					db.getDefaultColumnFamily().close();
					db.close();
				}))
				.whenComplete(toLogger(logger, TRACE, TRACE, WARN, thisMethod()));
	}

	@Override
	public Promise<? extends KvStorage> create(PubKey pubKey, String table) {
		Tuple2<PubKey, String> key = new Tuple2<>(pubKey, table);
		return handlesMap.computeIfAbsent(key, $ -> Promise.ofBlockingCallable(executor,
				() -> {
					ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(encodeAsArray(DESCRIPTOR_CODEC, key));
					ColumnFamilyHandle handle = db.createColumnFamily(cfd);
					return new RocksDbKvStorage(executor, db, handle);
				})
				.whenException(e -> handlesMap.remove(key))
				.whenComplete(toLogger(logger, TRACE, TRACE, WARN, thisMethod(), pubKey, table))
		);
	}
}
