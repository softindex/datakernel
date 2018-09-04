package io.datakernel.remotefs;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MockFsClient implements FsClient {

	@Override
	public Stage<SerialConsumer<ByteBuf>> upload(String filename, long offset) {
		if (offset == -1) {
			return Stage.ofException(new RemoteFsException("FileAlreadyExistsException"));
		}
		return Stage.of(SerialConsumer.of(AsyncConsumer.of(ByteBuf::recycle)));
	}

	@Override
	public Stage<SerialSupplier<ByteBuf>> download(String filename, long offset, long length) {
		return Stage.of(SerialSupplier.of(ByteBuf.wrapForReading("mock file".substring((int) offset, length == -1 ? 9 : (int) (offset + length)).getBytes(UTF_8))));
	}

	@Override
	public Stage<Set<String>> move(Map<String, String> changes) {
		return Stage.of(Collections.emptySet());
	}

	@Override
	public Stage<Set<String>> copy(Map<String, String> changes) {
		return Stage.of(Collections.emptySet());
	}

	@Override
	public Stage<List<FileMetadata>> list(String glob) {
		return Stage.of(Collections.emptyList());
	}

	@Override
	public Stage<Void> delete(String glob) {
		return Stage.ofException(new RemoteFsException("no files to delete"));
	}
}
