package io.global.globalfs.server;

import io.global.globalfs.api.GlobalFsName;

public interface CheckpointStorageFactory {
	CheckpointStorage create(GlobalFsName name);
}
