package io.global.fs.local;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.remotefs.FsClient;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.GlobalNamespace;
import io.global.fs.api.CheckpointStorage;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.transformers.FramesFromStorage;
import io.global.fs.transformers.FramesIntoStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static io.datakernel.async.Promises.asPromises;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.toLogger;
import static io.global.util.Utils.tolerantCollectBoolean;
import static java.util.stream.Collectors.toList;

public final class LocalGlobalFsNamespace extends GlobalNamespace<LocalGlobalFsNamespace, LocalGlobalFsNode, GlobalFsNode> {
	private static final Logger logger = LoggerFactory.getLogger(LocalGlobalFsNamespace.class);

	private final FsClient storage;
	private final CheckpointStorage checkpointStorage;

	public LocalGlobalFsNamespace(LocalGlobalFsNode node, PubKey space) {
		super(node, space);
		storage = node.getStorageFactory().apply(space);
		checkpointStorage = node.getCheckpointStorageFactory().apply(space);
	}

	public Promise<Void> streamDataFrames(GlobalFsNode from, GlobalFsNode to, String filename, long position, long revision) {
		// shortcut for when we need to stream the whole file
		if (position == 0) {
			return from.download(space, filename, position, -1)
					.then(supplier ->
							to.upload(space, filename, position, revision)
									.then(supplier::streamTo));
		}
		return from.download(space, filename, position, 0)
				.then(supplier -> supplier.toCollector(toList()))
				.then(frames -> {
					// shortcut for when we landed exactly on the checkpoint
					if (frames.size() == 1) {
						return from.download(space, filename, position, -1)
								.then(supplier ->
										to.upload(space, filename, position, revision)
												.then(supplier::streamTo));
					}
					// or we landed in between two and received some buf frames surrounded with checkpoints
					SignedData<GlobalFsCheckpoint> signedStartCheckpoint = frames.get(0).getCheckpoint();
					List<DataFrame> bufs = frames.subList(1, frames.size() - 1);
					SignedData<GlobalFsCheckpoint> signedEndCheckpoint = frames.get(frames.size() - 1).getCheckpoint();

					GlobalFsCheckpoint startCheckpoint = signedStartCheckpoint.getValue();
					GlobalFsCheckpoint endCheckpoint = signedEndCheckpoint.getValue();

					return getMetadata(filename)
							.then(signedCheckpoint -> {
								int offset = (int) (position - startCheckpoint.getPosition());

								Iterator<DataFrame> iterator = bufs.iterator();
								ByteBuf partialBuf = iterator.next().getBuf();
								while (partialBuf.readRemaining() < offset && iterator.hasNext()) {
									offset -= partialBuf.readRemaining();
									partialBuf.recycle();
									partialBuf = iterator.next().getBuf();
								}

								partialBuf.moveHead(offset);
								ByteBuf finalBuf = partialBuf;

								return from.download(space, filename, endCheckpoint.getPosition(), -1)
										.then(supplier ->
												to.upload(space, filename, position, revision)
														.then(ChannelSuppliers.concat(
																ChannelSupplier.of(DataFrame.of(signedCheckpoint), DataFrame.of(finalBuf)),
																ChannelSupplier.ofIterator(iterator),
																supplier
														)::streamTo));
							});
				});
	}

	public Promise<ChannelConsumer<DataFrame>> upload(String filename, long offset, long revision) {
		logger.trace("uploading to local storage {}, offset: {}", filename, offset);
		return checkpointStorage.drop(filename, revision)
				.then($ -> storage.upload(filename, offset, revision))
				.map(consumer -> consumer.transformWith(FramesIntoStorage.create(filename, space, checkpointStorage)));
	}

	public Promise<ChannelSupplier<DataFrame>> download(String fileName, long offset, long length) {
		logger.trace("downloading local copy of {} at {}, offset: {}, length: {}", fileName, space, offset, length);
		return checkpointStorage.loadIndex(fileName)
				.then(checkpoints -> {
					assert Arrays.equals(checkpoints, Arrays.stream(checkpoints).sorted().toArray()) : "Checkpoint array must be sorted!";

					int[] extremes = GlobalFsCheckpoint.getExtremes(checkpoints, offset, length);
					int start = extremes[0];
					int finish = extremes[1];
					return storage.download(fileName, checkpoints[start], checkpoints[finish] - checkpoints[start])
							.map(supplier -> supplier.transformWith(FramesFromStorage.create(fileName, checkpointStorage, checkpoints, start, finish)));
				});
	}

	public Promise<Void> delete(SignedData<GlobalFsCheckpoint> tombstone) {
		assert tombstone.getValue().isTombstone() : "trying to drop file with non-tombstone checkpoint";
		String filename = tombstone.getValue().getFilename();
		Promise<Void> checkpoint = checkpointStorage.loadMetaCheckpoint(filename)
				.then(old -> {
					if (old == null || GlobalFsCheckpoint.COMPARATOR.compare(tombstone.getValue(), old.getValue()) > 0) {
						return checkpointStorage.store(filename, tombstone);
					}
					return Promise.complete();
				});
		return Promises.all(checkpoint, storage.delete(filename, tombstone.getValue().getRevision()));
	}

	public Promise<List<SignedData<GlobalFsCheckpoint>>> list(String glob) {
		return checkpointStorage.listMetaCheckpoints(glob)
				.then(list ->
						Promises.reduce(toList(), 1, asPromises(list.stream()
								.map(filename -> AsyncSupplier.cast(() -> checkpointStorage.loadMetaCheckpoint(filename))))));
	}

	public Promise<SignedData<GlobalFsCheckpoint>> getMetadata(String fileName) {
		return checkpointStorage.loadMetaCheckpoint(fileName);
	}

	public Promise<Boolean> push(GlobalFsNode into, String glob) {
		return list(glob)
				.then(files -> tolerantCollectBoolean(files, signedLocalMeta -> {
					GlobalFsCheckpoint localMeta = signedLocalMeta.getValue();
					String filename = localMeta.getFilename();

					return into.getMetadata(space, filename)
							.then(signedRemoteMeta -> {
								GlobalFsCheckpoint remoteMeta = signedRemoteMeta != null ? signedRemoteMeta.getValue() : null;
								if (remoteMeta != null && GlobalFsCheckpoint.COMPARATOR.compare(localMeta, remoteMeta) < 0) {
									return Promise.of(false);
								}
								if (localMeta.isTombstone()) {
									if (remoteMeta != null && remoteMeta.isTombstone()) {
										logger.trace("both local and remote files {} are tombstones", remoteMeta.getFilename());
										return Promise.of(false);
									} else {
										logger.info("local file {} is a tombstone, removing remote", localMeta.getFilename());
										return into.delete(space, signedLocalMeta)
												.map($ -> true);
									}
								} else {
									if (remoteMeta != null && remoteMeta.isTombstone()) {
										logger.info("remote file {} is a tombstone, removing local", remoteMeta.getFilename());
										return delete(signedLocalMeta)
												.map($ -> true);
									}
									logger.info("pushing local file {} to node {}", localMeta.getFilename(), into);
									return streamDataFrames(node, into, filename, remoteMeta != null ? remoteMeta.getPosition() : 0, localMeta.getRevision())
											.map($ -> true);
								}
							});
				}))
				.acceptEx(toLogger(logger, TRACE, "push", space, into, node));
	}

	public Promise<Boolean> fetch(GlobalFsNode from, String glob) {
		return from.listEntities(space, glob)
				.then(files -> tolerantCollectBoolean(files, signedRemoteMeta -> {
							GlobalFsCheckpoint remoteMeta = signedRemoteMeta.getValue();
							String filename = remoteMeta.getFilename();
							return getMetadata(filename)
									.then(signedLocalMeta -> {
										GlobalFsCheckpoint localMeta = signedLocalMeta != null ? signedLocalMeta.getValue() : null;
										if (localMeta != null) {
											// our file is better
											if (GlobalFsCheckpoint.COMPARATOR.compare(localMeta, remoteMeta) >= 0) {
												logger.trace("local file {} is better than remote", localMeta.getFilename());
												return Promise.of(false);
											}
											// other file is encrypted with different key
											if (!Objects.equals(localMeta.getSimKeyHash(), remoteMeta.getSimKeyHash())) {
												logger.trace("remote file {} is encrypted with different key, ignoring", remoteMeta.getFilename());
												return Promise.of(false);
											}
										}
										if (remoteMeta.isTombstone()) {
											logger.trace("remote file {} is a tombstone with higher revision, removing local", remoteMeta.getFilename());
											return delete(signedRemoteMeta)
													.map($ -> true);
										}
										logger.info("remote file {} is better than local", remoteMeta.getFilename());

										long ourSize = localMeta != null ? localMeta.getPosition() : 0;

										assert remoteMeta.getPosition() >= ourSize : "Remote meta position is cannot be less than our size at this point";

										return streamDataFrames(from, node, filename, ourSize, remoteMeta.getRevision())
												.map($ -> true);
									});
						}
				))
				.acceptEx(toLogger(logger, TRACE, "fetch", space, from, node));
	}
}
