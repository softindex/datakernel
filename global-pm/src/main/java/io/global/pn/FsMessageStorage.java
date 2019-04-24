package io.global.pn;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FsClient;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.pn.api.MessageStorage;
import io.global.pn.api.RawMessage;
import io.global.pn.util.BinaryDataFormats;
import org.jetbrains.annotations.Nullable;

public final class FsMessageStorage implements MessageStorage {
	private static final String EXT = ".msg";

	private final FsClient storage;

	public FsMessageStorage(FsClient storage) {
		this.storage = storage;
	}

	private String getFileName(PubKey space, long id) {
		return space.asString() + '/' + Long.toHexString(id) + EXT;
	}

	@Override
	public Promise<Void> store(PubKey space, SignedData<RawMessage> message) {
		return storage.upload(getFileName(space, message.getValue().getId()))
				.then(consumer -> consumer.accept(BinaryUtils.encode(BinaryDataFormats.SIGNED_RAW_MSG_CODEC, message), null));
	}

	@Override
	public Promise<@Nullable SignedData<RawMessage>> load(PubKey space) {
		return storage.list(space.asString() + "/*" + EXT)
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
	public Promise<Void> delete(PubKey space, long id) {
		return storage.delete(getFileName(space, id));
	}
}
