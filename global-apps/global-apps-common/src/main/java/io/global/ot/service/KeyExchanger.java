package io.global.ot.service;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.EventloopService;
import io.global.common.PrivKey;

import java.util.Map;

public interface KeyExchanger extends EventloopService {
	Promise<Map<String, PrivKey>> receiveKeys();

	Promise<Void> sendKeys(Map<String, PrivKey> keys);
}
