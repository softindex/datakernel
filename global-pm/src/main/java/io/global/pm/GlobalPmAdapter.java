package io.global.pm;

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.pm.api.Message;
import io.global.pm.api.PmClient;
import org.jetbrains.annotations.Nullable;

public final class GlobalPmAdapter<T> implements PmClient<T> {
	private final GlobalPmDriver<T> driver;
	private final KeyPair keys;

	public GlobalPmAdapter(GlobalPmDriver<T> driver, PrivKey privKey) {
		this.driver = driver;
		this.keys = privKey.computeKeys();
	}

	@Override
	public Promise<Void> send(PubKey receiver, String mailBox, T payload) {
		return driver.send(keys.getPrivKey(), receiver, mailBox, payload);
	}

	@Override
	public Promise<ChannelConsumer<T>> multisend(PubKey receiver, String mailBox) {
		return driver.multisend(keys.getPrivKey(), receiver, mailBox);
	}

	@Override
	public Promise<ChannelSupplier<Message<T>>> stream(String mailBox) {
		return driver.stream(keys, mailBox, 0);
	}

	@Override
	public Promise<@Nullable Message<T>> poll(String mailBox) {
		return driver.poll(keys, mailBox);
	}

	@Override
	public Promise<ChannelSupplier<Message<T>>> multipoll(String mailBox, long timestamp) {
		return driver.multipoll(keys, mailBox, timestamp);
	}

	@Override
	public Promise<Void> drop(String mailBox, long id) {
		return driver.drop(keys, mailBox, id);
	}

	@Override
	public Promise<ChannelConsumer<Long>> multidrop(String mailBox) {
		return driver.multidrop(keys, mailBox);
	}
}
