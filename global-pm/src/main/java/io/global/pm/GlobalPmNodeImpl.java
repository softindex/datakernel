package io.global.pm;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.promise.RetryPolicy;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.pm.GlobalPmNamespace.MailBox;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.MessageStorage;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.global.util.Utils.tolerantCollectVoid;
import static io.global.util.Utils.untilTrue;

public final class GlobalPmNodeImpl extends AbstractGlobalNode<GlobalPmNodeImpl, GlobalPmNamespace, GlobalPmNode> implements GlobalPmNode {
	public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.immediateRetry().withMaxTotalRetryCount(10);
	public static final Duration DEFAULT_SYNC_MARGIN = Duration.ofMinutes(5);

	private Duration syncMargin = DEFAULT_SYNC_MARGIN;
	private final MessageStorage storage;
	RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;

	// region creators
	private GlobalPmNodeImpl(RawServerId id, DiscoveryService discoveryService,
			Function<RawServerId, GlobalPmNode> nodeFactory,
			MessageStorage storage) {
		super(id, discoveryService, nodeFactory);
		this.storage = storage;
	}

	public static GlobalPmNodeImpl create(RawServerId id, DiscoveryService discoveryService,
			Function<RawServerId, GlobalPmNode> nodeFactory, MessageStorage storage) {
		return new GlobalPmNodeImpl(id, discoveryService, nodeFactory, storage);
	}

	public GlobalPmNodeImpl withSyncMargin(Duration syncMargin) {
		this.syncMargin = syncMargin;
		return this;
	}

	public GlobalPmNodeImpl withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}
	// endregion

	@Override
	protected GlobalPmNamespace createNamespace(PubKey space) {
		return new GlobalPmNamespace(this, space);
	}

	public Duration getSyncMargin() {
		return syncMargin;
	}

	public MessageStorage getStorage() {
		return storage;
	}

	@Override
	public Promise<ChannelConsumer<SignedData<RawMessage>>> upload(PubKey space, String mailBox) {
		GlobalPmNamespace ns = ensureNamespace(space);
		MailBox box = ns.ensureMailBox(mailBox);
		return box.upload()
				.map(consumer -> consumer.withAcknowledgement(ack -> ack
						.whenComplete(() -> {
							if (!isMasterFor(space)) {
								box.push();
							}
						})));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String mailBox, long timestamp) {
		MailBox box = ensureNamespace(space).ensureMailBox(mailBox);
		return storage.download(space, mailBox, timestamp)
				.then(supplier -> {
					if (supplier != null) {
						return Promise.of(supplier);
					}
					return simpleMethod(space,
							master -> master.download(space, mailBox, timestamp)
									.map(messageSupplier -> {
										ChannelSplitter<SignedData<RawMessage>> splitter = ChannelSplitter.create();
										ChannelOutput<SignedData<RawMessage>> output = splitter.addOutput();
										splitter.addOutput().set(ChannelConsumer.ofPromise(box.upload()));
										splitter.getInput().set(messageSupplier);
										return output.getSupplier();
									}),
							$ -> Promise.of(ChannelSupplier.of())
					);
				});
	}

	@Override
	public Promise<Void> send(PubKey space, String mailBox, SignedData<RawMessage> message) {
		MailBox box = ensureNamespace(space).ensureMailBox(mailBox);
		return box.send(message)
				.toVoid()
				.whenResult($ -> {
					if (!isMasterFor(space)) {
						box.push();
					}
				});
	}

	@Override
	public Promise<@Nullable SignedData<RawMessage>> poll(PubKey space, String mailBox) {
		GlobalPmNamespace ns = ensureNamespace(space);
		MailBox box = ns.ensureMailBox(mailBox);
		return box.poll()
				.then(message -> {
					if (message != null) {
						return Promise.of(message);
					}
					return simpleMethod(space,
							master -> master.poll(space, mailBox)
									.then(polledMsg -> {
										if (polledMsg == null) {
											return Promise.ofException(new StacklessException("No message polled"));
										}
										return box.send(polledMsg)
												.map($ -> polledMsg)
												.whenResult($ -> box.fetch(master));
									}),
							$ -> Promise.of(null));
				});
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return simpleMethod(space, node -> node.list(space), GlobalPmNamespace::list);
	}

	public Promise<Void> fetch() {
		return Promises.all(namespaces.values().stream().map(GlobalPmNamespace::updateMailBoxes))
				.then($ -> forEachMailBox(MailBox::fetch));
	}

	public Promise<Void> fetch(String mailbox) {
		return Promises.all(namespaces.keySet().stream()
				.map(space -> fetch(space, mailbox)));
	}

	public Promise<Void> fetch(PubKey space, String mailbox) {
		return ensureNamespace(space).ensureMailBox(mailbox).fetch();
	}

	public Promise<Void> push() {
		return forEachMailBox(MailBox::push);
	}

	private final AsyncSupplier<Void> catchUp = reuse(this::doCatchUp);

	public Promise<Void> catchUp() {
		return catchUp.get();
	}

	private Promise<Void> doCatchUp() {
		return untilTrue(() -> {
			long timestampBegin = now.currentTimeMillis();
			return fetch()
					.map($2 -> now.currentTimeMillis() <= timestampBegin + latencyMargin.toMillis());
		});
	}

	private Promise<Void> forEachMailBox(Function<MailBox, Promise<Void>> fn) {
		return tolerantCollectVoid(new HashSet<>(namespaces.values()).stream().flatMap(entry -> entry.getMailBoxes().values().stream()), fn);
	}

	@Override
	public String toString() {
		return "GlobalPmNodeImpl{id=" + id + '}';
	}
}
