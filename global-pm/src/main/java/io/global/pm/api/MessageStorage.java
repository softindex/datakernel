package io.global.pm.api;

import io.datakernel.async.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import org.jetbrains.annotations.Nullable;

public interface MessageStorage {

	Promise<Void> store(PubKey space, String mailBox, SignedData<RawMessage> message);

	Promise<@Nullable SignedData<RawMessage>> load(PubKey space, String mailBox);

	Promise<Void> delete(PubKey space, String mailBox, long id);
}
