package io.global.globalfs.api;

import io.datakernel.async.Stage;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.common.PubKey;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface GlobalFsClient {
	Stage<Set<String>> getFsNames(PubKey pubKey);

	Stage<SerialSupplier<DataFrame>> downloadFromThis(GlobalFsName id, String filename, long offset, long limit);

	Stage<SerialSupplier<DataFrame>> download(GlobalFsName id, String filename, long offset, long limit);

	default SerialSupplier<DataFrame> downloadStream(GlobalFsName id, String filename, long offset, long limit) {
		return SerialSupplier.ofStage(download(id, filename, offset, limit));
	}

	Stage<SerialConsumer<DataFrame>> upload(GlobalFsName id, String filename, long offset);

	default SerialConsumer<DataFrame> uploadStream(GlobalFsName id, String filename, long offset) {
		return SerialConsumer.ofStage(upload(id, filename, offset));
	}

	Stage<Revision> list(GlobalFsName id, long revisionId);

	Stage<List<FileMetadata>> list(GlobalFsName id, String glob);

	Stage<Void> delete(GlobalFsName name, String glob);

	Stage<Set<String>> copy(GlobalFsName name, Map<String, String> changes);

	Stage<Set<String>> move(GlobalFsName name, Map<String, String> changes);
}
