package io.global.pm;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelOutput;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.csp.queue.ChannelZeroBuffer;
import io.datakernel.util.ref.RefBoolean;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AbstractGlobalNode;
import io.global.common.api.DiscoveryService;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.MessageStorage;
import io.global.pm.api.RawMessage;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.global.util.Utils.nSuccessesOrLess;

public final class GlobalPmNodeImpl extends AbstractGlobalNode<GlobalPmNodeImpl, GlobalPmNamespace, GlobalPmNode> implements GlobalPmNode {
	public static final Duration DEFAULT_SYNC_MARGIN = Duration.ofMinutes(5);

	private int uploadCallNumber = 1;
	private int uploadSuccessNumber = 0;

	private boolean doesDownloadCaching = true;
	private boolean doesUploadCaching = false;

	private Duration syncMargin = DEFAULT_SYNC_MARGIN;

	private final MessageStorage storage;

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

	public GlobalPmNodeImpl withUploadCall(int number) {
		this.uploadCallNumber = number;
		return this;
	}

	public GlobalPmNodeImpl withUploadSuccess(int number) {
		this.uploadSuccessNumber = number;
		return this;
	}

	public GlobalPmNodeImpl withDownloadCaching(boolean downloadCaching) {
		this.doesDownloadCaching = downloadCaching;
		return this;
	}

	public GlobalPmNodeImpl withUploadCaching(boolean uploadCaching) {
		this.doesUploadCaching = uploadCaching;
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
		return ns.ensureMasterNodes()
				.then(masters -> {
					if (isMasterFor(space)) {
						return ns.upload(mailBox);
					}
					return nSuccessesOrLess(uploadCallNumber, masters.stream()
							.map(master -> AsyncSupplier.cast(() -> master.upload(space, mailBox))))
							.map(consumers -> {
								ChannelZeroBuffer<SignedData<RawMessage>> buffer = new ChannelZeroBuffer<>();

								ChannelSplitter<SignedData<RawMessage>> splitter = ChannelSplitter.create(buffer.getSupplier())
										.lenient();

								RefBoolean localCompleted = new RefBoolean(false);
								if (doesUploadCaching || consumers.isEmpty()) {
									splitter.addOutput().set(ChannelConsumer.ofPromise(ns.upload(mailBox))
											.withAcknowledgement(ack ->
													ack.whenComplete(($, e) -> {
														if (e == null) {
															localCompleted.set(true);
														} else {
															splitter.close(e);
														}
													})));
								} else {
									localCompleted.set(true);
								}

								Promise<Void> process = splitter.splitInto(consumers, uploadSuccessNumber, localCompleted);
								return buffer.getConsumer().withAcknowledgement(ack -> ack.both(process));
							});
				});

	}

	@Override
	public Promise<ChannelSupplier<SignedData<RawMessage>>> download(PubKey space, String mailBox, long timestamp) {
		GlobalPmNamespace ns = ensureNamespace(space);
		return ns.download(mailBox, timestamp)
				.then(supplier -> {
					if (supplier != null) {
						return Promise.of(supplier);
					}
					return ns.ensureMasterNodes()
							.then(masters -> {
								if (isMasterFor(space) || masters.isEmpty()) {
									return ns.download(mailBox, timestamp);
								}
								if (!doesDownloadCaching) {
									return Promises.firstSuccessful(masters.stream()
											.map(node -> AsyncSupplier.cast(() ->
													node.download(space, mailBox, timestamp))));
								}
								return Promises.firstSuccessful(masters.stream()
										.map(node -> AsyncSupplier.cast(() ->
												Promises.toTuple(node.download(space, mailBox, timestamp), node.upload(space, mailBox))
														.map(t -> {
															ChannelSplitter<SignedData<RawMessage>> splitter = ChannelSplitter.create();
															ChannelOutput<SignedData<RawMessage>> output = splitter.addOutput();
															splitter.addOutput().set(t.getValue2());
															splitter.getInput().set(t.getValue1());
															return output.getSupplier();
														}))));
							});
				});
	}

	@Override
	public Promise<@Nullable SignedData<RawMessage>> poll(PubKey space, String mailBox) {
		GlobalPmNamespace ns = ensureNamespace(space);
		return ns.ensureMasterNodes()
				.then(masters -> {
					if (isMasterFor(space) || masters.isEmpty()) {
						return ns.poll(mailBox);
					}
					return fetch(space).thenEx(($, e) -> ns.poll(mailBox));
				});
	}

	@Override
	public Promise<Set<String>> list(PubKey space) {
		return simpleMethod(space, node -> node.list(space), ns -> ns.list(space));
	}

	public Promise<Void> fetch() {
		return Promises.all(getManagedPublicKeys().stream().map(this::fetch));
	}

	public Promise<Void> push() {
		return Promises.all(namespaces.keySet().stream().map(this::push));
	}

	public Promise<Void> fetch(PubKey space) {
		return ensureNamespace(space).fetch();
	}

	public Promise<Void> push(PubKey space) {
		return ensureNamespace(space).push();
	}

	private final AsyncSupplier<Void> catchUp = reuse(this::doCatchUp);

	public Promise<Void> catchUp() {
		return catchUp.get();
	}

	private Promise<Void> doCatchUp() {
		return Promises.until($ -> {
			long timestampBegin = now.currentTimeMillis();
			return fetch()
					.map($2 -> now.currentTimeMillis() <= timestampBegin + latencyMargin.toMillis());

		});
	}

	@Override
	public String toString() {
		return "GlobalPmNodeImpl{id=" + id + '}';
	}
}
