package io.global.pm;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.csp.AbstractChannelSupplier;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.csp.queue.ChannelQueue;
import io.datakernel.eventloop.ScheduledRunnable;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNamespace;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;
import io.global.pm.util.PmStreamChannelBuffer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

import static io.datakernel.async.function.AsyncSuppliers.coalesce;
import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.datakernel.async.process.AsyncExecutors.retry;
import static io.datakernel.common.Utils.nullify;
import static io.datakernel.common.collection.CollectionUtils.difference;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.promise.Promises.toList;
import static io.global.util.Utils.tolerantCollectVoid;
import static java.util.stream.Collectors.toSet;

public final class GlobalPmNamespace extends AbstractGlobalNamespace<GlobalPmNamespace, GlobalPmNodeImpl, GlobalPmNode> {
	private final Map<String, MailBox> mailBoxes = new HashMap<>();
	private final AsyncSupplier<Void> updateMailBoxes = reuse(this::doUpdateMailBoxes);
	private boolean listUpdated;

	private long updateMailBoxesTimestamp;

	public GlobalPmNamespace(GlobalPmNodeImpl node, PubKey space) {
		super(node, space);
	}

	public MailBox ensureMailBox(String mailBox) {
		return mailBoxes.computeIfAbsent(mailBox, box -> {
			MailBox mailBoxEntry = new MailBox(box);
			mailBoxEntry.start();
			return mailBoxEntry;
		});
	}

	public Map<String, MailBox> getMailBoxes() {
		return mailBoxes;
	}

	public Promise<Void> updateMailBoxes() {
		return updateMailBoxes.get();
	}

	private Promise<Void> doUpdateMailBoxes() {
		if (updateMailBoxesTimestamp > node.getCurrentTimeProvider().currentTimeMillis() - node.getLatencyMargin().toMillis()) {
			return Promise.complete();
		}
		return ensureMasterNodes()
				.then(masters -> toList(masters.stream()
						.map(master -> master.list(space)
								.thenEx((v, e) -> Promise.of(e == null ? v : Collections.<String>emptySet()))))
						.map(lists -> lists.stream().flatMap(Collection::stream).collect(toSet())))
				.whenResult(repoNames -> repoNames.forEach(this::ensureMailBox))
				.whenResult($ -> updateMailBoxesTimestamp = node.getCurrentTimeProvider().currentTimeMillis())
				.toVoid();
	}

	public Promise<Set<String>> list() {
		if (listUpdated) {
			return Promise.of(mailBoxes.keySet());
		}
		return node.getStorage().list(space)
				.whenResult(list -> {
					list.forEach(this::ensureMailBox);
					listUpdated = true;
				});
	}

	class MailBox {
		private final AsyncSupplier<Void> fetch = reuse(this::doFetch);
		private final AsyncSupplier<Void> push = coalesce(AsyncSupplier.cast(this::doPush).withExecutor(retry(node.retryPolicy)));
		private final Map<RawServerId, PmMasterRepository> pmMasterRepos = new HashMap<>();
		private ScheduledRunnable scheduledUpdateStreams;

		private final String mailBox;

		private long lastFetchTimestamp;
		private long lastPushTimestamp;

		private final Set<ChannelQueue<SignedData<RawMessage>>> buffers = new HashSet<>();

		MailBox(String mailBox) {
			this.mailBox = mailBox;
		}

		void start() {
			if (node.streamMasterRepositories) {
				updateStreams();
			}
		}

		void stop() {
			scheduledUpdateStreams = nullify(scheduledUpdateStreams, ScheduledRunnable::cancel);
			pmMasterRepos.values().forEach(PmMasterRepository::closeStream);
		}

		private void updateStreams() {
			long currentTimestamp = node.getCurrentTimeProvider().currentTimeMillis();
			long fetchFromTimestamp = Math.max(0, lastFetchTimestamp - node.getSyncMargin().toMillis());
			ensureMasterNodes()
					.whenResult($1 -> {
						difference(pmMasterRepos.keySet(), masterNodes.keySet())
								.forEach(key -> pmMasterRepos.remove(key).closeStream());
						difference(masterNodes.keySet(), pmMasterRepos.keySet())
								.forEach(serverId -> {
									PmMasterRepository masterRepository = new PmMasterRepository(serverId, space, mailBox, masterNodes.get(serverId));
									pmMasterRepos.put(serverId, masterRepository);
									masterRepository.createStream(fetchFromTimestamp)
											.streamTo(upload())
											.whenException(e -> pmMasterRepos.remove(serverId));
								});
					})
					.whenComplete(() -> {
						lastFetchTimestamp = currentTimestamp;
						if (scheduledUpdateStreams != null) {
							scheduledUpdateStreams = getCurrentEventloop().delay(node.getLatencyMargin(), this::updateStreams);
						}
					});
		}

		Promise<ChannelConsumer<SignedData<RawMessage>>> upload() {
			return node.getStorage().upload(space, mailBox)
					.map(consumer -> consumer.peek(signedData -> buffers.forEach(buffer -> buffer.put(signedData))));
		}

		Promise<@Nullable SignedData<RawMessage>> poll() {
			return node.getStorage().poll(space, mailBox);
		}

		Promise<Boolean> send(SignedData<RawMessage> message) {
			buffers.forEach(buffer -> buffer.put(message));
			return node.getStorage().put(space, mailBox, message);
		}

		Promise<Void> push() {
			return push.get();
		}

		Promise<Void> fetch() {
			return fetch.get();
		}

		Promise<ChannelSupplier<SignedData<RawMessage>>> stream(long timestamp) {
			PmStreamChannelBuffer buffer = new PmStreamChannelBuffer(timestamp);
			buffers.add(buffer);
			return node.getStorage().download(space, mailBox, timestamp)
					.map(downloadSupplier -> ChannelSuppliers.concat(
							downloadSupplier == null ? ChannelSupplier.of() : downloadSupplier.peek(buffer::update),
							new AbstractChannelSupplier<SignedData<RawMessage>>() {
								@Override
								protected Promise<SignedData<RawMessage>> doGet() {
									return buffer.take();
								}

								@Override
								protected void onClosed(@NotNull Throwable e) {
									buffers.remove(buffer);
									buffer.close(e);
								}
							}));
		}

		private Promise<Void> doPush() {
			return forEachMaster(this::push);
		}

		private Promise<Void> doFetch() {
			return forEachMaster(this::fetch);
		}

		Promise<Void> fetch(GlobalPmNode from) {
			long currentTimestamp = node.getCurrentTimeProvider().currentTimeMillis();
			long fetchFromTimestamp = Math.max(0, lastFetchTimestamp - node.getSyncMargin().toMillis());
			return ChannelSupplier.ofPromise(from.download(space, mailBox, fetchFromTimestamp))
					.streamTo(upload())
					.whenResult($ -> lastFetchTimestamp = currentTimestamp);
		}

		Promise<Void> push(GlobalPmNode to) {
			long currentTimestamp = node.getCurrentTimeProvider().currentTimeMillis();
			long pushFromTimestamp = Math.max(0, lastPushTimestamp - node.getSyncMargin().toMillis());
			return node.getStorage().download(space, mailBox, pushFromTimestamp)
					.then(supplier -> {
						if (supplier == null) {
							return Promise.complete();
						}
						return supplier.streamTo(to.upload(space, mailBox));
					})
					.whenResult($ -> lastPushTimestamp = currentTimestamp);
		}

		private Promise<Void> forEachMaster(Function<GlobalPmNode, Promise<Void>> action) {
			return ensureMasterNodes()
					.then(masters -> tolerantCollectVoid(masters, action));
		}
	}
}
