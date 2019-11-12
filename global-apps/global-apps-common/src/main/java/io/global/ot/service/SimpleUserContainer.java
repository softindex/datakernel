package io.global.ot.service;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.ot.session.KvSessionStore;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.NotNull;

public final class SimpleUserContainer implements UserContainer {
	private final Eventloop eventloop;
	private final KeyPair keys;
	private final KvSessionStore<UserId> sessionStore;

	private SimpleUserContainer(Eventloop eventloop, KeyPair keys, KvSessionStore<UserId> sessionStore) {
		this.eventloop = eventloop;
		this.keys = keys;
		this.sessionStore = sessionStore;
	}

	public static SimpleUserContainer create(Eventloop eventloop, KeyPair keys, KvSessionStore<UserId> sessionStore) {
		return new SimpleUserContainer(eventloop, keys, sessionStore);
	}

	@Override
	public KeyPair getKeys() {
		return keys;
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<Void> start() {
		return sessionStore.start();
	}

	@Override
	public @NotNull Promise<Void> stop() {
		return sessionStore.stop();
	}

	@Override
	public SessionStore<UserId> getSessionStore() {
		return sessionStore;
	}
}
