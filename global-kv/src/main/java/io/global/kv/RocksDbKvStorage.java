package io.global.kv;

import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.global.common.SignedData;
import io.global.kv.api.KvStorage;
import io.global.kv.api.RawKvItem;
import org.rocksdb.*;

import java.util.concurrent.Executor;

import static io.global.kv.util.Utils.packValue;
import static io.global.kv.util.Utils.unpackValue;

public class RocksDbKvStorage implements KvStorage {
	private final Executor executor;
	private final RocksDB db;
	private final ColumnFamilyHandle handle;

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

	private Promise<RocksIterator> iterator() {
		return Promise.ofBlockingCallable(executor, () -> {
			RocksIterator iterator = db.newIterator(handle);
			iterator.seekToFirst();
			return iterator;
		});
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawKvItem>>> download(long timestamp) {
		return iterator().map(iterator ->
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
	public Promise<ChannelConsumer<SignedData<byte[]>>> remove() {
		return Promise.of(ChannelConsumer.<SignedData<byte[]>>of(this::remove)
				.withAcknowledgement(ack -> ack.then($ -> flush())));
	}

	@Override
	public Promise<SignedData<RawKvItem>> get(byte[] key) {
		return Promise.ofBlockingCallable(executor, () -> unpackValue(key, db.get(handle, key)));
	}

	@Override
	public Promise<Void> put(SignedData<RawKvItem> item) {
		return Promise.ofBlockingRunnable(executor, () -> doPut(item));
	}

	@Override
	public Promise<Void> remove(SignedData<byte[]> key) {
		return Promise.ofBlockingRunnable(executor, () -> db.delete(handle, key.getValue()));
	}
}
