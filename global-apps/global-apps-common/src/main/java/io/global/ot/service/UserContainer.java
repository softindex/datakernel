package io.global.ot.service;

import io.datakernel.eventloop.EventloopService;
import io.datakernel.http.session.SessionStore;
import io.global.common.KeyPair;
import io.global.ot.session.UserId;

public interface UserContainer extends EventloopService {
	KeyPair getKeys();

	SessionStore<UserId> getSessionStore();
}
