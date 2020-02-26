package io.global.crdt;

import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamSplitter;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractRepoGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.common.api.RepoStorageFactory;

import java.util.Set;
import java.util.function.Function;

public final class GlobalCrdtNodeImpl extends AbstractRepoGlobalNode<GlobalCrdtNodeImpl, GlobalCrdtNamespace, GlobalCrdtNode, CrdtStorage> implements GlobalCrdtNode {
	private final RepoStorageFactory<CrdtStorage> storageFactory;

	public GlobalCrdtNodeImpl(RawServerId id, DiscoveryService discoveryService, Function<RawServerId, GlobalCrdtNode> nodeFactory,
			RepoStorageFactory<CrdtStorage> storageFactory) {
		super(id, discoveryService, nodeFactory);
		this.storageFactory = storageFactory;
	}

	public RepoStorageFactory<CrdtStorage> getStorageFactory() {
		return storageFactory;
	}

	@Override
	protected GlobalCrdtNamespace createNamespace(PubKey space) {
		return new GlobalCrdtNamespace(this, space);
	}

	@Override
	public Promise<StreamConsumer<SignedData<RawCrdtData>>> upload(PubKey space, String table) {
		GlobalCrdtNamespace ns = ensureNamespace(space);
		return ns.ensureRepository(table)
				.then(repo -> repo.getStorage().upload()
						.map(consumer -> consumer
								.withAcknowledgement(ack -> ack
										.whenComplete(() -> {
											if (!isMasterFor(space)) {
												repo.push();
											}
										}))));
	}

	@Override
	public Promise<StreamSupplier<SignedData<RawCrdtData>>> download(PubKey space, String repo, long timestamp) {
		return simpleRepoMethod(space, repo,
				r -> r.getStorage().download(timestamp),
				(master, r) -> master.download(space, repo, timestamp)
						.map(itemSupplier -> {
							StreamSplitter<SignedData<RawCrdtData>> splitter = StreamSplitter.create();
							StreamSupplier<SignedData<RawCrdtData>> output = splitter.newOutput();

							splitter.newOutput().streamTo(StreamConsumer.ofPromise(r.getStorage().upload()));

							itemSupplier.streamTo(splitter.getInput());
							return output;
						}),
				() -> Promise.of(StreamSupplier.of()));
	}

	@Override
	public Promise<StreamConsumer<SignedData<byte[]>>> remove(PubKey space, String repo) {
		return simpleRepoMethod(space, repo,
				r -> r.getStorage().remove(),
				(master, r) -> master.remove(space, repo),
				() -> Promise.of(StreamConsumer.idle()));
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return Promise.of(ensureNamespace(space).getRepos().keySet());
	}
}
