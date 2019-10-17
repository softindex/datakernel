package io.global.pm;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.util.Tuple2;
import io.datakernel.util.Tuple4;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.MessageStorage;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

import static io.datakernel.util.Utils.arrayStartsWith;
import static io.global.pm.util.RocksDbUtils.*;

public final class RocksDbMessageStorage implements MessageStorage, EventloopService {
	private final Eventloop eventloop;
	private final Executor executor;
	private final String storagePath;

	private TransactionDB db;

	private RocksDbMessageStorage(Eventloop eventloop, Executor executor, String storagePath) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.storagePath = storagePath;
	}

	public static RocksDbMessageStorage create(Eventloop eventloop, Executor executor, String storagePath) {
		return new RocksDbMessageStorage(eventloop, executor, storagePath);
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<?> start() {
		return Promise.ofBlockingRunnable(executor, () -> {
			try (Options options = new Options().setCreateIfMissing(true);
					TransactionDBOptions transactionOptions = new TransactionDBOptions()) {
				db = TransactionDB.open(options, transactionOptions, storagePath);
			}
		});
	}

	@Override
	public @NotNull Promise<?> stop() {
		return Promise.ofBlockingRunnable(executor, () -> db.close());
	}

	@Override
	public Promise<Boolean> put(PubKey space, String mailBox, SignedData<RawMessage> message) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					RawMessage newValue = message.getValue();
					byte[] oldKey = null;
					RawMessage oldValue = null;
					try (RocksIterator iterator = db.newIterator()) {
						byte[] prefix = prefixedByMailBox(space, mailBox);
						for (iterator.seek(prefix); iterator.isValid() && arrayStartsWith(iterator.key(), prefix); iterator.next()) {
							RawMessage oldMessage = unpack(iterator.key(), iterator.value()).getValue();
							if (newValue.getId() == oldMessage.getId()) {
								oldKey = iterator.key();
								oldValue = oldMessage;
								break;
							}
						}
					}
					if (oldKey == null) {
						Tuple2<byte[], byte[]> packed = pack(space, mailBox, message);
						db.put(packed.getValue1(), packed.getValue2());
						return true;
					}
					if (oldValue.isTombstone() || oldValue.getTimestamp() > newValue.getTimestamp()) {
						return false;
					}

					try (WriteBatch writeBatch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions()) {
						writeBatch.delete(oldKey);
						Tuple2<byte[], byte[]> packed = pack(space, mailBox, message);
						writeBatch.put(packed.getValue1(), packed.getValue2());
						db.write(writeOptions, writeBatch);
					}
					return true;
				});
	}

	@Override
	public Promise<@Nullable SignedData<RawMessage>> poll(PubKey space, String mailBox) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (RocksIterator iterator = db.newIterator()) {
						byte[] prefix = prefixedByMailBox(space, mailBox);
						for (iterator.seek(prefix); iterator.isValid() && arrayStartsWith(iterator.key(), prefix); iterator.next()) {
							SignedData<RawMessage> message = unpack(iterator.key(), iterator.value());
							if (message.getValue().isMessage()) {
								return message;
							}
						}
						return null;
					}
				});
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String mailBox, long timestamp) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					RocksIterator iterator = db.newIterator();
					byte[] prefixTimestamp = prefixedByTimestamp(space, mailBox, timestamp);
					byte[] prefixMailbox = prefixedByMailBox(space, mailBox);
					iterator.seek(prefixTimestamp);
					return ChannelSupplier.of(() ->
							Promise.ofBlockingCallable(executor,
									() -> {
										if (iterator.isValid() && arrayStartsWith(iterator.key(), prefixMailbox)) {
											SignedData<RawMessage> signedDbItem = unpack(iterator.key(), iterator.value());
											iterator.next();
											return signedDbItem;
										}
										return null;
									}))
							.withEndOfStream(eos -> eos.whenComplete(iterator::close));
				});
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (RocksIterator iterator = db.newIterator()) {
						Set<String> result = new HashSet<>();
						byte[] prefix = prefixedBySpace(space);
						for (iterator.seek(prefix); iterator.isValid() && arrayStartsWith(iterator.key(), prefix); iterator.next()) {
							Tuple4<PubKey, String, Long, Long> tuple = unpackKey(iterator.key());
							result.add(tuple.getValue2());
						}
						return result;
					}
				});
	}

	@Override
	public Promise<Void> cleanup(long timestamp) {
		return Promise.ofBlockingRunnable(executor,
				() -> {
					try (RocksIterator iterator = db.newIterator()) {
						for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
							RawMessage rawMessage = unpack(iterator.key(), iterator.value()).getValue();
							if (rawMessage.isTombstone() && rawMessage.getTimestamp() < timestamp) {
								db.delete(iterator.key());
							}
						}
					}
				});
	}
}
