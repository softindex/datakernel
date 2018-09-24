package io.global.globalfs.server;

import io.global.globalfs.api.GlobalFsFileSystem;
import io.global.globalfs.api.GlobalFsNamespace;

@FunctionalInterface
public interface FileSystemFactory {
	GlobalFsFileSystem create(GlobalFsNamespace group, String fsName);
}
