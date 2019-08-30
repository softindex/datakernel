package io.global.ot.service;

import io.datakernel.async.Promise;
import io.global.common.PrivKey;

import java.util.Set;

public interface ContainerKeyManager {
	Promise<Set<PrivKey>> getKeys();

	Promise<Void> updateKeys(Set<PrivKey> keys);
}
