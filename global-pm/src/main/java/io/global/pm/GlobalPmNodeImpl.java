package io.global.pm;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.pm.GlobalPmNamespace.Repo;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.MessageStorage;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.async.function.AsyncSuppliers.reuse;
import static io.global.util.Utils.tolerantCollectVoid;
import static io.global.util.Utils.untilTrue;

public final class GlobalPmNodeImpl extends AbstractGlobalNode<GlobalPmNodeImpl, GlobalPmNamespace, GlobalPmNode> implements GlobalPmNode {
	private final MessageStorage storage;

	private GlobalPmNodeImpl(RawServerId id, DiscoveryService discoveryService, Function<RawServerId, GlobalPmNode> nodeFactory, MessageStorage storage) {
		super(id, discoveryService, nodeFactory);
		this.storage = storage;
	}

	public static GlobalPmNodeImpl create(RawServerId id, DiscoveryService discoveryService, Function<RawServerId, GlobalPmNode> nodeFactory, MessageStorage storage) {
		return new GlobalPmNodeImpl(id, discoveryService, nodeFactory, storage);
	}

	@Override
	protected GlobalPmNamespace createNamespace(PubKey space) {
		return new GlobalPmNamespace(this, space);
	}

	public MessageStorage getStorage() {
		return storage;
	}

	@Override
	public Promise<ChannelConsumer<SignedData<RawMessage>>> upload(PubKey space, String mailBox) {
		GlobalPmNamespace ns = ensureNamespace(space);
		Repo box = ns.ensureRepo(mailBox);
		return box.upload()
				.map(consumer -> consumer.withAcknowledgement(ack -> ack
						.whenComplete(() -> {
							if (!isMasterFor(space)) {
								box.push();
							}
						})));
	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String repo, long timestamp) {
		return simpleMethod(space,
				ns -> storage.download(space, repo, timestamp),
				(master, ns) -> master.download(space, repo, timestamp)
						.map(messageSupplier -> {
							ChannelSplitter<SignedData<RawMessage>> splitter = ChannelSplitter.create();
							ChannelOutput<SignedData<RawMessage>> output = splitter.addOutput();
							splitter.addOutput().set(ChannelConsumer.ofPromise(ns.ensureRepo(repo).upload()));
							splitter.getInput().set(messageSupplier);
							return output.getSupplier();
						}),
				() -> Promise.of(ChannelSupplier.of()));
	}

	@Override
	public Promise<Void> send(PubKey space, String mailBox, SignedData<RawMessage> message) {
		Repo box = ensureNamespace(space).ensureRepo(mailBox);
		return box.send(message)
				.toVoid()
				.whenResult($ -> {
					if (!isMasterFor(space)) {
						box.push();
					}
				});
	}

	@Override
	public Promise<@Nullable SignedData<RawMessage>> poll(PubKey space, String repo) {
		return simpleMethod(space,
				ns -> ns.ensureRepo(repo).poll(),
				(master, ns) -> master.poll(space, repo)
						.then(polledMsg -> {
							Repo box = ns.ensureRepo(repo);
							return polledMsg != null ?
									box.send(polledMsg)
											.map($ -> polledMsg)
											.whenResult($ -> box.fetch(master)) :
									Promise.ofException(new StacklessException("No message polled"));
						}),
				() -> Promise.of(null));
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		GlobalPmNamespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.then(nodes -> {
					if (isMasterFor(space)) {
						return ns.list();
					}
					return Promises.firstSuccessful(Stream.concat(nodes.stream().map(globalFsNode -> () -> list(space)), Stream.of(ns::list)));
				});
	}

	public Promise<Void> fetch() {
		return Promises.all(namespaces.values().stream().map(GlobalPmNamespace::updateMailBoxes))
				.then($ -> forEachMailBox(Repo::fetch));
	}

	public Promise<Void> fetch(String mailbox) {
		return Promises.all(namespaces.keySet().stream()
				.map(space -> fetch(space, mailbox)));
	}

	public Promise<Void> fetch(PubKey space, String mailbox) {
		return ensureNamespace(space).ensureRepo(mailbox).fetch();
	}

	public Promise<Void> push() {
		return forEachMailBox(Repo::push);
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

	private Promise<Void> forEachMailBox(Function<Repo, Promise<Void>> fn) {
		return tolerantCollectVoid(new HashSet<>(namespaces.values()).stream().flatMap(entry -> entry.getMailBoxes().values().stream()), fn);
	}

	@Override
	public String toString() {
		return "GlobalPmNodeImpl{id=" + id + '}';
	}
}
