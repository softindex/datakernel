package io.global.common.api;

import io.datakernel.promise.Promise;
import io.global.common.PubKey;

@FunctionalInterface
public interface RepoStorageFactory<S> {

	Promise<? extends S> create(PubKey space, String repo);
}
