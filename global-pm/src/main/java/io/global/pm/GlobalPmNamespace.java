package io.global.pm;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNamespace;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;

import static io.datakernel.async.function.AsyncSuppliers.coalesce;
import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.datakernel.async.process.AsyncExecutors.retry;
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
		return mailBoxes.computeIfAbsent(mailBox, MailBox::new);
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

		private final String mailBox;

		private long lastFetchTimestamp;
		private long lastPushTimestamp;

		MailBox(String mailBox) {
			this.mailBox = mailBox;
		}

		Promise<ChannelConsumer<SignedData<RawMessage>>> upload() {
			return node.getStorage().upload(space, mailBox);
		}

		Promise<@Nullable SignedData<RawMessage>> poll() {
			return node.getStorage().poll(space, mailBox);
		}

		Promise<Boolean> send(SignedData<RawMessage> message) {
			return node.getStorage().put(space, mailBox, message);
		}

		Promise<Void> push() {
			return push.get();
		}

		Promise<Void> fetch() {
			return fetch.get();
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
					.streamTo(node.getStorage().upload(space, mailBox))
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
