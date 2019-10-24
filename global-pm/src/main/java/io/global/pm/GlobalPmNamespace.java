package io.global.pm;

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNamespace;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class GlobalPmNamespace extends AbstractGlobalNamespace<GlobalPmNamespace, GlobalPmNodeImpl, GlobalPmNode> {
	private final Map<String, MailBox> mailBoxes = new HashMap<>();
	private boolean listUpdated;

	public GlobalPmNamespace(GlobalPmNodeImpl node, PubKey space) {
		super(node, space);
	}

	public MailBox ensureMailBox(String mailBox) {
		return mailBoxes.computeIfAbsent(mailBox, MailBox::new);
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

	public Promise<Void> fetch() {
		return ensureMasterNodes()
				.then(masters ->
						Promises.all(masters.stream()
								.map(master -> master.list(space)
										.then(mailBoxes -> Promises.all(mailBoxes.stream()
												.map(mailBox -> ensureMailBox(mailBox).fetch(master)))))));
	}

	public Promise<Void> push() {
		return ensureMasterNodes()
				.then(masters ->
						Promises.all(masters
								.stream()
								.map(master -> Promises.all(mailBoxes.values().stream()
										.map(mailBox -> mailBox.push(master))))));
	}

	class MailBox {
		private final String mailBox;
		private long lastFetchTimestamp;
		private long lastPushTimestamp;
		private long cacheTimestamp;

		MailBox(String mailBox) {
			this.mailBox = mailBox;
		}

		Promise<ChannelConsumer<SignedData<RawMessage>>> upload() {
			return node.getStorage().upload(space, mailBox)
					.whenResult($ -> cacheTimestamp = node.getCurrentTimeProvider().currentTimeMillis());
		}

		Promise<ChannelSupplier<SignedData<RawMessage>>> download(long timestamp) {
			return node.getStorage().download(space, mailBox, timestamp);
		}

		Promise<@Nullable SignedData<RawMessage>> poll() {
			return node.getStorage().poll(space, mailBox);
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
			return ChannelSupplier.ofPromise(node.getStorage().download(space, mailBox, pushFromTimestamp))
					.streamTo(ChannelConsumer.ofPromise(to.upload(space, mailBox)))
					.whenResult($ -> lastPushTimestamp = currentTimestamp);
		}

		boolean isCacheValid() {
			return node.getCurrentTimeProvider().currentTimeMillis() - cacheTimestamp < node.getLatencyMargin().toMillis();
		}
	}
}
