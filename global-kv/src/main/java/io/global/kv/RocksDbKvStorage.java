package io.global.kv;

import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.global.common.SignedData;
import io.global.kv.api.KvStorage;
import io.global.kv.api.RawKvItem;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.*;

import java.util.concurrent.Executor;

import static io.global.kv.util.Utils.packValue;
import static io.global.kv.util.Utils.unpackValue;

public class RocksDbKvStorage implements KvStorage {
	private final Executor executor;
	private final RocksDB db;
	private ColumnFamilyHandle handle;

	private final FlushOptions flushOptions;
	private final WriteOptions writeOptions;

	public RocksDbKvStorage(Executor executor, RocksDB db, ColumnFamilyHandle handle) {
		this.handle = handle;
		this.executor = executor;
		this.db = db;
		flushOptions = new FlushOptions();
		writeOptions = new WriteOptions().setDisableWAL(true);
	}

	public ColumnFamilyHandle getHandle() {
		return handle;
	}

	public Promise<Void> flush() {
		return Promise.ofBlockingRunnable(executor, () -> db.flush(flushOptions, handle));
	}

	private void doPut(SignedData<RawKvItem> signedDbItem) throws RocksDBException, ParseException {
		byte[] key = signedDbItem.getValue().getKey();
		byte[] value = db.get(handle, key);
		if (value != null) {
			SignedData<RawKvItem> old = unpackValue(key, value);
			if (old.getValue().getTimestamp() > signedDbItem.getValue().getTimestamp()) {
				return;
			}
		}
		db.put(handle, writeOptions, key, packValue(signedDbItem));
	}

	@Override
	public Promise<ChannelConsumer<SignedData<RawKvItem>>> upload() {
		return Promise.of(ChannelConsumer.<SignedData<RawKvItem>>of(signedDbItem -> Promise.ofBlockingRunnable(executor, () -> doPut(signedDbItem)))
				.withAcknowledgement(ack -> ack.then($ -> flush())));
	}

	private Promise<@Nullable RocksIterator> iterator() {
		return Promise.ofBlockingCallable(executor, () -> {
			RocksIterator iterator = db.newIterator(handle);
			iterator.seekToFirst();
			if (!iterator.isValid()) {
				iterator.close();
				return null;
			}
			return iterator;
		});
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawKvItem>>> download(long timestamp) {
		return iterator().map(iterator ->
				iterator == null ?
						null :
						ChannelSupplier.of(() ->
								Promise.ofBlockingCallable(executor, () -> {
									while (iterator.isValid()) {
										SignedData<RawKvItem> signedDbItem = unpackValue(iterator.key(), iterator.value());
										iterator.next();
										if (signedDbItem.getValue().getTimestamp() > timestamp) {
											return signedDbItem;
										}
									}
									return null;
								})));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawKvItem>>> download() {
		return iterator().map(iterator ->
				iterator == null ?
						null :
						ChannelSupplier.of(() ->
								Promise.ofBlockingCallable(executor, () -> {
									if (!iterator.isValid()) {
										return null;
									}
									SignedData<RawKvItem> signedDbItem = unpackValue(iterator.key(), iterator.value());
									iterator.next();
									return signedDbItem;
								})));
	}

	@Override
	public Promise<SignedData<RawKvItem>> get(byte[] key) {
		return Promise.ofBlockingCallable(executor, () -> {
			byte[] value = db.get(handle, key);
			return value == null ? null : unpackValue(key, value);
		});
	}

	@Override
	public Promise<Void> put(SignedData<RawKvItem> item) {
		return Promise.ofBlockingRunnable(executor, () -> doPut(item));
	}

	@Override
	public Promise<Void> reset() {
		return Promise.ofBlockingCallable(executor, () -> {
			ColumnFamilyDescriptor descriptor = handle.getDescriptor();
			db.dropColumnFamily(handle);
			handle = db.createColumnFamily(descriptor);
			return null;
		});
	}
}
