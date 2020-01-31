package io.global.pm.api;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

public interface MessageStorage {

	// True - if new message has overwritten previous or if there were no previous message with the same id
	Promise<Boolean> put(PubKey space, String mailBox, SignedData<RawMessage> message);

	// Does not return tombstones
	Promise<@Nullable SignedData<RawMessage>> poll(PubKey space, String mailBox);

	// null if nothing to download
	Promise<@Nullable ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String mailBox, long timestamp);

	default Promise<@Nullable ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String mailBox) {
		return download(space, mailBox, 0);
	}

	default Promise<ChannelConsumer<SignedData<RawMessage>>> upload(PubKey space, String mailBox) {
		return Promise.of(ChannelConsumer.of(message -> put(space, mailBox, message).toVoid()));
	}

	Promise<Set<String>> list(PubKey space);

	Promise<Void> cleanup(long timestamp);
}
