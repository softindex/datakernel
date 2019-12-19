package io.global.ot.service;

import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Optional;
import io.datakernel.promise.Promise;
import io.datakernel.remotefs.FsClient;
import io.global.fs.local.GlobalFsNodeImpl;

@Inject
public final class FsUserContainer extends AbstractUserContainer {
	@Inject
	private FsClient fsClient;
	@Inject
	private GlobalFsNodeImpl node;
	@Inject
	@Optional
	@Named("app-dir")
	private String appDir;

	public FsClient getFsClient() {
		return fsClient;
	}

	@Override
	protected Promise<Void> doStart() {
		return node.fetch(getKeys().getPubKey(), appDir == null ? "**" : appDir)
				.toTry()
				.toVoid();
	}

	@Override
	protected Promise<Void> doStop() {
		return Promise.complete();
	}
}
