package io.global.db;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.global.common.SignedData;
import io.global.db.api.DbStorage;
import io.global.db.util.Utils;
import org.rocksdb.*;

import java.util.concurrent.Executor;

public class RocksdbDbStorage implements DbStorage {
	private final Executor executor;
	private final RocksDB db;

	private final FlushOptions flushOptions;
	private final WriteOptions writeOptions;

	public RocksdbDbStorage(Executor executor, RocksDB db) {
		this.executor = executor;
		this.db = db;
		flushOptions = new FlushOptions();
		writeOptions = new WriteOptions().setDisableWAL(true);
	}

	public Promise<Void> flush() {
		return Promise.ofBlockingRunnable(executor, () -> {
			try {
				db.flush(flushOptions);
			} catch (RocksDBException e) {
				throw new UncheckedException(e);
			}
		});
	}

	private void doPut(SignedData<DbItem> signedDbItem) {
		byte[] key = signedDbItem.getValue().getKey();
		byte[] value;
		try {
			value = db.get(key);
		} catch (RocksDBException e) {
			throw new UncheckedException(e);
		}
		if (value != null) {
			SignedData<DbItem> old;
			try {
				old = Utils.unpackValue(key, value);
			} catch (ParseException e) {
				throw new UncheckedException(e);
			}
			if (old.getValue().getTimestamp() > signedDbItem.getValue().getTimestamp()) {
				return;
			}
		}
		try {
			db.put(writeOptions, key, Utils.packValue(signedDbItem));
		} catch (RocksDBException e) {
			throw new UncheckedException(e);
		}
	}

	private SignedData<DbItem> doGet(byte[] key) {
		try {
			return Utils.unpackValue(key, db.get(key));
		} catch (RocksDBException | ParseException e) {
			throw new UncheckedException(e);
		}
	}

	@Override
	public Promise<ChannelConsumer<SignedData<DbItem>>> upload() {
		return Promise.of(ChannelConsumer.<SignedData<DbItem>>of(signedDbItem -> Promise.ofBlockingRunnable(executor, () -> doPut(signedDbItem)))
				.withAcknowledgement(ack -> ack.thenCompose($ -> flush())));
	}

	private Promise<RocksIterator> iterator() {
		return Promise.ofBlockingCallable(executor, () -> {
			RocksIterator iterator = db.newIterator();
			iterator.seekToFirst();
			return iterator;
		});
	}

	@Override
	public Promise<ChannelSupplier<SignedData<DbItem>>> download(long timestamp) {
		return iterator().thenApply(iterator ->
				ChannelSupplier.of(() ->
						Promise.ofBlockingCallable(executor, () -> {
							while (iterator.isValid()) {
								byte[] key = iterator.key();
								SignedData<DbItem> signedDbItem = doGet(key);
								iterator.next();
								if (signedDbItem.getValue().getTimestamp() > timestamp) {
									return signedDbItem;
								}
							}
							return null;
						})));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<DbItem>>> download() {
		return iterator().thenApply(iterator ->
				ChannelSupplier.of(() ->
						Promise.ofBlockingCallable(executor, () -> {
							if (!iterator.isValid()) {
								return null;
							}
							SignedData<DbItem> signedDbItem = doGet(iterator.key());
							iterator.next();
							return signedDbItem;
						})));
	}

	@Override
	public Promise<ChannelConsumer<SignedData<byte[]>>> remove() {
		return Promise.of(
				ChannelConsumer.of(
						(SignedData<byte[]> key) -> Promise.ofBlockingRunnable(executor,
								() -> {
									try {
										db.delete(key.getValue());
									} catch (RocksDBException e) {
										throw new UncheckedException(e);
									}
								}))
						.withAcknowledgement(ack -> ack.thenCompose($ -> flush())));
	}

	@Override
	public Promise<SignedData<DbItem>> get(byte[] key) {
		return Promise.ofBlockingCallable(executor, () -> doGet(key));
	}

	@Override
	public Promise<Void> put(SignedData<DbItem> item) {
		return Promise.ofBlockingRunnable(executor, () -> doPut(item));
	}

	@Override
	public Promise<Void> remove(SignedData<byte[]> key) {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					try {
						db.delete(key.getValue());
					} catch (RocksDBException e) {
						throw new UncheckedException(e);
					}
				});
	}
}
