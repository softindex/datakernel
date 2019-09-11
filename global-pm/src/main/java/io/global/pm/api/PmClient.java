package io.global.pm.api;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.global.common.PubKey;
import org.jetbrains.annotations.Nullable;

public interface PmClient<T> {

	Promise<Long> send(PubKey receiver, String mailBox, long timestamp, T payload);

	Promise<ChannelConsumer<Message<T>>> multisend(PubKey receiver, String mailBox);

	Promise<@Nullable Message<T>> poll(String mailBox);

	Promise<ChannelSupplier<Message<T>>> multipoll(String mailBox);

	Promise<Void> drop(String mailBox, long id);

	Promise<ChannelConsumer<Long>> multidrop(String mailBox);
}
