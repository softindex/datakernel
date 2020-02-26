package io.global.crdt;

import io.datakernel.promise.Promise;
import io.global.common.PubKey;

@FunctionalInterface
public interface CrdtStorageFactory {
	Promise<? extends CrdtStorage> create(PubKey space, String table);
}
