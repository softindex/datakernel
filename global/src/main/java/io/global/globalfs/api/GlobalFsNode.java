package io.global.globalfs.api;

import io.datakernel.async.Stage;
import io.datakernel.exception.StacklessException;
import io.datakernel.serial.SerialSupplier;
import io.global.common.PubKey;

import java.time.Duration;
import java.util.Set;

/**
 * This component handles one of GlobalFS nodes.
 */
public interface GlobalFsNode {
	StacklessException RECURSIVE_ERROR = new StacklessException("Trying to download a file from a server that also tries to download this file.");

	Stage<Set<String>> getFilesystemNames(PubKey pubKey);

	Stage<GlobalFsNamespace> getNamespace(PubKey key);

	Stage<GlobalFsFileSystem> getFileSystem(GlobalFsName name);

	default Stage<GlobalFsFileSystem> getFileSystem(GlobalFsAddress address) {
		return getFileSystem(address.getFsName());
	}

	Stage<SerialSupplier<DataFrame>> download(GlobalFsAddress file, long offset, long limit);

	default SerialSupplier<DataFrame> downloadStream(GlobalFsAddress file, long offset, long limit) {
		return SerialSupplier.ofStage(download(file, offset, limit));
	}

	default Stage<GlobalFsMetadata> getMetadata(GlobalFsAddress address) {
		return getFileSystem(address)
				.thenCompose(fs -> fs.getMetadata(address.getFile()));
	}

	Settings getSettings();

	interface Settings {

		Duration getLatencyMargin();
	}
}
