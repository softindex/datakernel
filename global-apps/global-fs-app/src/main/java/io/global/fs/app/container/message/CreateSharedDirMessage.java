package io.global.fs.app.container.message;

import io.global.fs.app.container.SharedDirMetadata;

public final class CreateSharedDirMessage implements SharedDirMessage {
	private final SharedDirMetadata dirMetadata;

	public CreateSharedDirMessage(SharedDirMetadata dirMetadata) {
		this.dirMetadata = dirMetadata;
	}

	public String getId() {
		return dirMetadata.getDirId();
	}

	@Override
	public SharedDirMetadata getDirMetadata() {
		return dirMetadata;
	}
}
