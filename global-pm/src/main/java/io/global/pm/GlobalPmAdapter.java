package io.global.pm;

import io.datakernel.async.Promise;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.pm.api.Message;
import io.global.pm.api.PmClient;
import org.jetbrains.annotations.Nullable;

public final class GlobalPmAdapter<T> implements PmClient<T> {
	private final GlobalPmDriver<T> driver;
	private final PrivKey privKey;
	private final KeyPair keys;

	public GlobalPmAdapter(GlobalPmDriver<T> driver, PrivKey privKey) {
		this.driver = driver;
		this.privKey = privKey;
		this.keys = privKey.computeKeys();
	}

	@Override
	public Promise<Void> send(PubKey receiver, String mailBox, T payload) {
		return driver.send(privKey, receiver, mailBox, payload);
	}

	@Override
	public Promise<@Nullable Message<T>> poll(String mailBox) {
		return driver.poll(keys, mailBox);
	}

	@Override
	public Promise<Void> drop(String mailBox, long id) {
		return driver.drop(keys, mailBox, id);
	}
}
