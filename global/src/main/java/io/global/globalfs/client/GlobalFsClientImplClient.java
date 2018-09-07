package io.global.globalfs.client;

import io.datakernel.async.Stage;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.common.PubKey;
import io.global.globalfs.api.DataFrame;
import io.global.globalfs.api.GlobalFsClient;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.Revision;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GlobalFsClientImplClient implements GlobalFsClient {

	/* THIS CLIENT WILL DO NETWORKING OF SOME SORT TO COMMUNICATE WITH GlobalFsClientLocalImpl,
       OR OTHER GlobalFsClient IMPLEMENTATION ON SERVER */

	@Override
	public Stage<Set<String>> getFsNames(PubKey pubKey) {
		throw new UnsupportedOperationException("GlobalFsClientImplClient#getFsNames is not implemented yet");
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> downloadFromThis(GlobalFsName id, String filename, long offset, long limit) {
		throw new UnsupportedOperationException("GlobalFsClientImplClient#downloadFromThis is not implemented yet");
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> download(GlobalFsName id, String filename, long offset, long limit) {
		throw new UnsupportedOperationException("GlobalFsClientImplClient#download is not implemented yet");
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(GlobalFsName id, String filename, long offset) {
		throw new UnsupportedOperationException("GlobalFsClientImplClient#upload is not implemented yet");
	}

	@Override
	public Stage<Revision> list(GlobalFsName id, long revisionId) {
		throw new UnsupportedOperationException("GlobalFsClientImplClient#list is not implemented yet");
	}

	@Override
	public Stage<List<FileMetadata>> list(GlobalFsName id, String glob) {
		throw new UnsupportedOperationException("GlobalFsClientImplClient#list is not implemented yet");
	}

	@Override
	public Stage<Void> delete(GlobalFsName name, String glob) {
		throw new UnsupportedOperationException("GlobalFsClientImplClient#delete is not implemented yet");
	}

	@Override
	public Stage<Set<String>> copy(GlobalFsName name, Map<String, String> changes) {
		throw new UnsupportedOperationException("GlobalFsClientImplClient#copy is not implemented yet");
	}

	@Override
	public Stage<Set<String>> move(GlobalFsName name, Map<String, String> changes) {
		throw new UnsupportedOperationException("GlobalFsClientImplClient#move is not implemented yet");
	}
}
