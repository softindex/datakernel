package io.global.common.api;

import io.datakernel.promise.Promise;
import io.global.common.PubKey;

import java.util.Set;

public interface PubKeyStorage {
	Promise<Set<PubKey>> loadPublicKeys();

	Promise<Void> storePublicKey(PubKey pubKey);

	Promise<Void> removePublicKey(PubKey pubKey);
}
