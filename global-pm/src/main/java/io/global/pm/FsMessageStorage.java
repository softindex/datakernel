package io.global.pm;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FsClient;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pm.api.MessageStorage;
import io.global.pm.api.RawMessage;
import io.global.pm.util.BinaryDataFormats;
import org.jetbrains.annotations.Nullable;

public final class FsMessageStorage implements MessageStorage {
	private static final String EXT = ".msg";

	private final FsClient storage;

	private FsMessageStorage(FsClient storage) {
		this.storage = storage;
	}

	public static FsMessageStorage create(FsClient storage) {
		return new FsMessageStorage(storage);
	}

	private String getFileName(PubKey space, String mailBox, long id) {
		return space.asString() + '/' + mailBox + '/' + Long.toHexString(id) + EXT;
	}

	@Override
	public Promise<Void> store(PubKey space, String mailBox, SignedData<RawMessage> message) {
		return storage.upload(getFileName(space, mailBox, message.getValue().getId()))
				.then(consumer -> consumer.accept(BinaryUtils.encode(BinaryDataFormats.SIGNED_RAW_MSG_CODEC, message), null));
	}

	@Override
	public Promise<@Nullable SignedData<RawMessage>> load(PubKey space, String mailBox) {
		return storage.list(space.asString() + '/' + mailBox + "/*" + EXT)
				.then(list -> {
					if (list.isEmpty()) {
						return Promise.of(null);
					}
					return storage.download(list.get(0).getName())
							.then(supplier -> supplier.toCollector(ByteBufQueue.collector()))
							.then(buf -> {
								try {
									return Promise.of(BinaryUtils.decode(BinaryDataFormats.SIGNED_RAW_MSG_CODEC, buf));
								} catch (ParseException e) {
									return Promise.ofException(e);
								}
							});
				});
	}

	@Override
	public Promise<Void> delete(PubKey space, String mailBox, long id) {
		return storage.delete(getFileName(space, mailBox, id));
	}
}
