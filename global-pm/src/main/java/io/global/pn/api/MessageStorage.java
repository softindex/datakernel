package io.global.pn.api;

import io.datakernel.async.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import org.jetbrains.annotations.Nullable;

public interface MessageStorage {

	Promise<Void> store(PubKey space, SignedData<RawMessage> message);

	Promise<@Nullable SignedData<RawMessage>> load(PubKey space);

	Promise<Void> delete(PubKey space, long id);
}
