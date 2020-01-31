package io.global.ot.service;

import io.datakernel.async.service.EventloopService;
import io.datakernel.promise.Promise;
import io.global.common.PrivKey;

import java.util.Map;

public interface KeyExchanger extends EventloopService {
	Promise<Map<String, PrivKey>> receiveKeys();

	Promise<Void> sendKeys(Map<String, PrivKey> keys);
}
