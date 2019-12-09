package io.global.ot.service;

import io.datakernel.di.annotation.Inject;
import io.datakernel.promise.Promise;

@Inject
public final class SimpleUserContainer extends AbstractUserContainer {

	@Override
	protected Promise<Void> doStart() {
		return Promise.complete();
	}

	@Override
	protected Promise<Void> doStop() {
		return Promise.complete();
	}
}
