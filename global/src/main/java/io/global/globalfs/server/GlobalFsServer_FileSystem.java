package io.global.globalfs.server;

import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.remotefs.FsClient;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamTransformer;
import io.datakernel.time.CurrentTimeProvider;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsServer;
import io.global.globalfs.api.GlobalFsServer.DataFrame;

import java.time.Duration;
import java.util.List;

import static io.datakernel.async.AsyncSuppliers.reuse;

final class GlobalFsServer_FileSystem {
	private final GlobalFsName name;
	private final FsClient fsClient;
	private final CheckpointStorage checkpointStorage;
	private final AsyncSupplier<List<GlobalFsServer>> ensureServers;
	private Settings settings;

	public interface Settings {
		Duration getLatencyMargin();
	}

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	long updateTimestamp;

	private final AsyncSupplier<Void> update = reuse(null);
	private final AsyncSupplier<Void> catchUp = reuse(this::catchUp);

	GlobalFsServer_FileSystem(FsClient fsClient, CheckpointStorage checkpointStorage, GlobalFsName name, AsyncSupplier<List<GlobalFsServer>> ensureServers) {
		this.fsClient = fsClient;
		this.name = name;
		this.checkpointStorage = checkpointStorage;
		this.ensureServers = ensureServers;
	}

	public Stage<List<GlobalFsServer>> ensureServers() {
		return ensureServers.get();
	}

	private Stage<Void> catchUp() {
		return Stage.ofCallback(this::catchUpImpl);
	}

	private void catchUpImpl(Callback<Void> cb) {
		long timestampBegin = now.currentTimeMillis();
		fetch()
				.thenRun(() -> {
					long timestampEnd = now.currentTimeMillis();
					if (timestampEnd - timestampBegin > settings.getLatencyMargin().toMillis()) {
						cb.set(null);
					} else {
						catchUpImpl(cb);
					}
				})
				.whenException(cb::setException);
	}

	public Stage<Void> fetch() {
		return ensureServers()
				.thenCompose(servers -> Stages.firstSuccessful(servers.stream().map(this::fetch)));
	}

	public Stage<Void> fetch(GlobalFsServer server) {
		return server.list(name, "**")
				.thenCompose(files -> Stages.runSequence(files.stream()
						.map(file ->
								server.downloadStream(name, file.getName(), 0, file.getSize())
										.streamTo(StreamConsumer.ofSerialConsumer(SerialConsumer.of(
												dataFrame -> {
													return null;
												})))
										.getProducerResult())));
	}

	// TODO (abulah)
	private final class Downloader implements StreamTransformer<ByteBuf, DataFrame> {
		final ByteBufQueue queue = new ByteBufQueue();
		private SettableStage<DataFrame> queueDataAvailable;

		@Override
		public StreamConsumer<ByteBuf> getInput() {
			return null;
		}

		@Override
		public StreamProducer<DataFrame> getOutput() {
			return null;
		}

		protected Stage<Void> consume(ByteBuf buf) {
			queue.add(buf);
			return Stage.complete();
		}

		protected Stage<DataFrame> produce() {
			if (queue.isEmpty()) {
				queueDataAvailable = new SettableStage<>();
				return queueDataAvailable;
			}
			ByteBuf buf = queue.take();
			return Stage.of(DataFrame.ofByteBuf(buf));
		}

	}

	// TODO (abulah)
	private final class Uploader implements StreamTransformer<DataFrame, ByteBuf> {
		final ByteBufQueue queue = new ByteBufQueue();

		protected Stage<Void> consume(DataFrame dataFrame) {
			if (dataFrame.isBuf()) {
				queue.add(dataFrame.getBuf());
			} else {
			}
			return null;
		}

		protected Stage<ByteBuf> produce() {
			ByteBuf buf = queue.take();
			return Stage.of(buf);
		}

		@Override
		public StreamConsumer<DataFrame> getInput() {
			return null;
		}

		@Override
		public StreamProducer<ByteBuf> getOutput() {
			return null;
		}
	}

	public Stage<StreamProducer<DataFrame>> download(String filename, long offset, long length) {
		return fsClient.download(filename, offset, length)
				.thenApply(producer -> producer.with(new Downloader()));
	}

	public Stage<StreamConsumer<DataFrame>> upload(String filename, long offset) {
		return fsClient.upload(filename, offset)
				.thenApply(producer -> producer.with(new Uploader()));
	}

}
