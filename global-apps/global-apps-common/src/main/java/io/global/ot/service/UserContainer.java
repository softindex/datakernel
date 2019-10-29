package io.global.ot.service;

import io.datakernel.async.service.EventloopService;
import io.global.common.KeyPair;

public interface UserContainer extends EventloopService {
	KeyPair getKeys();
}
