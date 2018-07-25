package io.datakernel.remotefs;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerWithResult;

import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MockFsClient implements FsClient {

	@Override
	public Stage<StreamConsumerWithResult<ByteBuf, Void>> upload(String filename, long offset) {
		if (offset == -1) {
			return Stage.ofException(new RemoteFsException("FileAlreadyExistsException"));
		}
		return Stage.of(StreamConsumer.ofConsumer(ByteBuf::recycle).withEndOfStreamAsResult());
	}

	@Override
	public Stage<StreamProducerWithResult<ByteBuf, Void>> download(String filename, long offset, long length) {
		return Stage.of(StreamProducer.of(ByteBuf.wrapForReading("mock file".substring((int) offset, length == -1 ? 9 : (int) (offset + length)).getBytes(UTF_8))).withEndOfStreamAsResult());
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
