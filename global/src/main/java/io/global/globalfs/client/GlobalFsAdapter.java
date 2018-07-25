package io.global.globalfs.client;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.processor.StreamMap;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsServer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.global.globalfs.api.GlobalFsServer.DataFrame.ofByteBuf;

public class GlobalFsAdapter implements FsClient {
	private final GlobalFsServer server;
	private final GlobalFsName name;

	public GlobalFsAdapter(GlobalFsServer server, GlobalFsName name) {
		this.server = server;
		this.name = name;
	}

	@Override
	public Stage<StreamConsumerWithResult<ByteBuf, Void>> upload(String filename, long offset) {
		return server.upload(name, filename, offset)
				.thenApply(consumer -> consumer.with(StreamMap.create(
						(buf, output) -> {
							// TODO (abulah)
							output.onData(ofByteBuf(buf));
						})));
	}

	@Override
	public Stage<StreamProducerWithResult<ByteBuf, Void>> download(String filename, long offset, long length) {
		return server.download(name, filename, offset, length)
				.thenApply(producer -> producer.with(StreamMap.create(
						(dataFrame, output) -> {
							// TODO (abulah)
							output.onData(dataFrame.getBuf());
						}
				)));
	}

	@Override
	public Stage<Set<String>> move(Map<String, String> changes) {
		return server.move(name, changes);
	}

	@Override
	public Stage<Set<String>> copy(Map<String, String> changes) {
		return server.copy(name, changes);
	}

	@Override
	public Stage<List<FileMetadata>> list(String glob) {
		return server.list(name, glob);
	}

	@Override
	public Stage<Void> delete(String glob) {
		return server.delete(name, glob);
	}
}
