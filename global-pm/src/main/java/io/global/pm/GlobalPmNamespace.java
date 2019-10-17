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

import java.util.HashSet;
import java.util.Set;

public final class GlobalPmNamespace extends AbstractGlobalNamespace<GlobalPmNamespace, GlobalPmNodeImpl, GlobalPmNode> {
	private Set<String> mailBoxes;
	private long lastFetchTimestamp = 0;
	private long lastPushTimestamp = 0;

	public GlobalPmNamespace(GlobalPmNodeImpl node, PubKey space) {
		super(node, space);
	}

	public Promise<ChannelConsumer<SignedData<RawMessage>>> upload(String mailBox) {
		return node.getStorage().upload(space, mailBox)
				.whenResult($ -> {
					if (mailBoxes != null) {
						mailBoxes.add(mailBox);
					}
				});
	}

	public Promise<ChannelSupplier<SignedData<RawMessage>>> download(String mailBox, long timestamp) {
		return node.getStorage().download(space, mailBox, timestamp);
	}

	public Promise<@Nullable SignedData<RawMessage>> poll(String mailBox) {
		return node.getStorage().poll(space, mailBox);
	}

	public Promise<Set<String>> list(PubKey space) {
		if (mailBoxes != null) {
			return Promise.of(mailBoxes);
		}
		return node.getStorage().list(space)
				.whenResult(list -> mailBoxes = new HashSet<>(list));
	}

	public Promise<Void> fetch() {
		long currentTimestamp = node.getCurrentTimeProvider().currentTimeMillis();
		long fetchFromTimestamp = lastFetchTimestamp - node.getSyncMargin().toMillis();
		return ensureMasterNodes()
				.then(masters ->
						Promises.all(masters.stream()
								.map(master -> master.list(space)
										.then(mailBoxes -> Promises.all(mailBoxes.stream()
												.map(mailBox -> ChannelSupplier.ofPromise(master.download(space, mailBox, fetchFromTimestamp))
														.streamTo(ChannelConsumer.ofPromise(node.getStorage().upload(space, mailBox)))
														.whenResult($ -> {
															if (this.mailBoxes != null) {
																this.mailBoxes.add(mailBox);
															}
														})))))))
				.whenResult($ -> lastFetchTimestamp = currentTimestamp);
	}

	public Promise<Void> push() {
		long currentTimestamp = node.getCurrentTimeProvider().currentTimeMillis();
		long pushFromTimestamp = lastPushTimestamp - node.getSyncMargin().toMillis();
		return ensureMasterNodes()
				.then(masters ->
						Promises.all(masters
								.stream()
								.map(master -> Promises.all(node.getStorage().list(space)
										.map(mailBoxes -> Promises.all(mailBoxes.stream()
												.map(mailBox -> ChannelSupplier.ofPromise(node.getStorage().download(space, mailBox, pushFromTimestamp))
														.streamTo(ChannelConsumer.ofPromise(master.upload(space, mailBox))))))))))
				.whenResult($ -> lastPushTimestamp = currentTimestamp);
	}
}
