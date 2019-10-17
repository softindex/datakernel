package io.global.pm.api;

import io.datakernel.async.Promise;
import io.global.common.PubKey;
import org.jetbrains.annotations.Nullable;

public interface PmClient<T> {

	Promise<Void> send(PubKey receiver, String mailBox, T payload);

	Promise<@Nullable Message<T>> poll(String mailBox);

	Promise<Void> drop(String mailBox, long id);
}
