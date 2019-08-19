package io.global.ot.service;

import io.datakernel.eventloop.EventloopService;
import io.global.common.KeyPair;

public interface UserContainer extends EventloopService {
	KeyPair getKeys();
}
