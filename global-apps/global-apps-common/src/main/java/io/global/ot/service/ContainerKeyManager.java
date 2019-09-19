package io.global.ot.service;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.EventloopService;
import io.global.common.PrivKey;

import java.util.Set;

public interface ContainerKeyManager extends EventloopService {
	Promise<Set<PrivKey>> getKeys();

	Promise<Void> updateKeys(Set<PrivKey> keys);
}
