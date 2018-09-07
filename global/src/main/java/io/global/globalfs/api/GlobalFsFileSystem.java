package io.global.globalfs.api;

import io.datakernel.async.Stage;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface GlobalFsFileSystem {
	Stage<Void> catchUp();

	Stage<Void> fetch();

	Stage<Void> fetch(GlobalFsClient client);

	Stage<SerialConsumer<DataFrame>> upload(String fileName, long offset);

	Stage<SerialSupplier<DataFrame>> download(String fileName, long offset, long length);

	Stage<Revision> list(long revisionId);

	Stage<List<FileMetadata>> list(String glob);

	Stage<Void> delete(String glob);

	Stage<Set<String>> copy(Map<String, String> changes);

	Stage<Set<String>> move(Map<String, String> changes);
}
