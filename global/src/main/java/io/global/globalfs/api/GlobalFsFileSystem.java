package io.global.globalfs.api;

import io.datakernel.async.Stage;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface represents a single file system in GlobalFS.
 */
public interface GlobalFsFileSystem {
	GlobalFsName getName();

	Stage<Void> catchUp();

	Stage<Void> fetch();

	Stage<Void> fetch(GlobalFsNode client);

	Stage<SerialConsumer<DataFrame>> upload(String file, long offset);

	default SerialConsumer<DataFrame> uploadSerial(String file, long offset) {
		return SerialConsumer.ofStage(upload(file, offset));
	}

	Stage<SerialSupplier<DataFrame>> download(String file, long offset, long length);

	default SerialSupplier<DataFrame> downloadSerial(String file, long offset, long length) {
		return SerialSupplier.ofStage(download(file, offset, length));
	}

	Stage<List<GlobalFsMetadata>> list(String glob);

	default Stage<GlobalFsMetadata> getMetadata(String file) {
		return list(file).thenApply(res -> res.size() == 1 ? res.get(0) : null);
	}

	Stage<Void> delete(String glob);

	Stage<Set<String>> copy(Map<String, String> changes);

	Stage<Set<String>> move(Map<String, String> changes);
}
