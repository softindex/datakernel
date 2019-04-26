package io.global.pn;

import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.pn.api.Message;
import io.global.pn.api.PmClient;
import org.jetbrains.annotations.Nullable;

public final class GlobalPmAdapter<T> implements PmClient<T> {
	private final GlobalPmDriver<T> driver;
	private final PrivKey privKey;
	private final PubKey pubKey;
	private final KeyPair keys;

	public GlobalPmAdapter(GlobalPmDriver<T> driver, PrivKey privKey, PubKey pubKey) {
		this.driver = driver;
		this.privKey = privKey;
		this.pubKey = pubKey;

		keys = new KeyPair(privKey, pubKey);
	}

	@Override
	public Promise<Void> send(PubKey receiver, String mailBox, long timestamp, T payload) {
		return driver.send(privKey, receiver, mailBox, Message.of(timestamp, pubKey, payload));
	}

	@Override
	public Promise<ChannelConsumer<Message<T>>> multisend(PubKey receiver, String mailBox) {
		return driver.multisend(privKey, receiver, mailBox);
	}

	@Override
	public Promise<@Nullable Message<T>> poll(String mailBox) {
		return driver.poll(keys, mailBox);
	}

	@Override
	public Promise<ChannelSupplier<Message<T>>> multipoll(String mailBox) {
		return driver.multipoll(keys, mailBox);
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
