package io.global.ot.service;

import io.datakernel.di.annotation.Inject;
import io.datakernel.promise.Promise;
import io.datakernel.remotefs.FsClient;
import io.global.api.AppDir;
import io.global.fs.local.GlobalFsNodeImpl;

@Inject
public final class FsUserContainer extends AbstractUserContainer {
	@Inject
	private FsClient fsClient;
	@Inject
	private GlobalFsNodeImpl node;
	@Inject
	@AppDir
	private String appDir;

	public FsClient getFsClient() {
		return fsClient;
	}

	@Override
	protected Promise<?> doStart() {
		node.fetch(getKeys().getPubKey(), appDir + "/**");
		return Promise.complete();
	}

	@Override
	protected Promise<?> doStop() {
		return Promise.complete();
	}
}
