package io.global.pm.api;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public interface PmClient<T> {

	Promise<Void> send(PubKey receiver, String mailBox, T payload);

	Promise<ChannelConsumer<T>> multisend(PubKey receiver, String mailBox);

	Promise<@Nullable Message<T>> poll(String mailBox);

	Promise<ChannelSupplier<Message<T>>> multipoll(String mailBox, long timestamp);

	default Promise<ChannelSupplier<Message<T>>> multipoll(String mailBox){
		return multipoll(mailBox, 0);
	}

	default Promise<List<Message<T>>> batchpoll(String mailBox, long timestamp){
		return multipoll(mailBox, timestamp).then(ChannelSupplier::toList);
	}

	default Promise<List<Message<T>>> batchpoll(String mailBox){
		return batchpoll(mailBox, 0);
	}

	Promise<Void> drop(String mailBox, long id);

	Promise<ChannelConsumer<Long>> multidrop(String mailBox);
}
