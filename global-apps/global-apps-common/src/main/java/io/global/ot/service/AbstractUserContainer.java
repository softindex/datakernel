package io.global.ot.service;

import io.datakernel.di.annotation.Inject;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.session.SessionStore;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.ot.session.UserId;
import io.global.session.KvSessionStore;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractUserContainer implements UserContainer {
	@Inject
	private Eventloop eventloop;
	@Inject
	private KeyPair keys;
	@Inject
	private KvSessionStore<UserId> sessionStore;

	@Override
	public KeyPair getKeys() {
		return keys;
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public final @NotNull Promise<?> start() {
		return sessionStore.start()
				.then($ -> doStart());
	}

	@Override
	public final @NotNull Promise<?> stop() {
		return sessionStore.stop()
				.then($ -> doStop());
	}

	protected abstract Promise<?> doStart();

	protected abstract Promise<?> doStop();

	@Override
	public SessionStore<UserId> getSessionStore() {
		return sessionStore;
	}
}
