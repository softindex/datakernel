package io.global.ot.service;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.EventloopService;
import io.global.common.PrivKey;

import java.util.Set;

public interface KeyExchanger extends EventloopService {
	Promise<Set<PrivKey>> receiveKeys();

	Promise<Void> sendKeys(Set<PrivKey> keys);
}
