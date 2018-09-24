package io.global.globalfs.server;

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.exception.StacklessException;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.SerialSplitter;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class GlobalFsLocalNode implements GlobalFsNode {
	private final Map<PubKey, GlobalFsNamespace> publicKeyGroups = new HashMap<>();
	private final Set<GlobalFsAddress> searchingForDownload = new HashSet<>();
	private final RawServerId id;
	private final DiscoveryService discoveryService;
	private final RawNodeFactory clientFactory;
	private final FileSystemFactory fileSystemFactory;
	private final Settings settings;

	public GlobalFsLocalNode(RawServerId id, DiscoveryService discoveryService, RawNodeFactory clientFactory, FileSystemFactory fileSystemFactory, Settings settings) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.clientFactory = clientFactory;
		this.fileSystemFactory = fileSystemFactory;
		this.settings = settings;
	}

	private GlobalFsNamespace getOrAddNamespace(PubKey key) {
		return publicKeyGroups.computeIfAbsent(key, $ -> new LocalGlobalFsNamespace(this, key));
	}

	@Override
	public Stage<GlobalFsNamespace> getNamespace(PubKey key) {
		return Stage.of(getOrAddNamespace(key));
	}

	@Override
	public Stage<GlobalFsFileSystem> getFileSystem(GlobalFsName name) {
		return getNamespace(name.getPubKey()).thenCompose(ns -> ns.getFileSystem(name.getFsName()));
	}

	// region getters
	public RawServerId getId() {
		return id;
	}

	public DiscoveryService getDiscoveryService() {
		return discoveryService;
	}

	public RawNodeFactory getClientFactory() {
		return clientFactory;
	}

	public FileSystemFactory getFileSystemFactory() {
		return fileSystemFactory;
	}

	@Override
	public Settings getSettings() {
		return settings;
	}
	// endregion

	@Override
	public Stage<Set<String>> getFilesystemNames(PubKey pubKey) {
		return publicKeyGroups.get(pubKey).getFilesystemNames();
	}

	private Stage<SerialSupplier<DataFrame>> download(GlobalFsNode node, GlobalFsFileSystem local, GlobalFsAddress address, long offset, long limit) {
		return node.getMetadata(address)
				.thenCompose(res -> {
					if (res == null) {
						return Stage.ofException(new StacklessException("no such file"));
					}
					return node.download(address, offset, limit);
				})
				.thenApply(supplier -> {
					SerialSplitter<DataFrame> splitter = SerialSplitter.<DataFrame>create()
							.withInput(supplier.transform(DataFrame::slice));

					SerialSupplier<DataFrame> output = splitter
							.addOutput()
							.getOutputSupplier()
							.transform(df -> {
								DataFrame slice = df.slice();
								df.recycle();
								return slice;
							});
					MaterializedStage<Void> cacheProcess = splitter.addOutput()
							.getOutputSupplier()
							.transform(df -> {
								DataFrame slice = df.slice();
								df.recycle();
								return slice;
							})
							.streamTo(local.uploadSerial(address.getFile(), offset))
							.materialize();
					splitter.start();
					return output.withEndOfStream(eos -> eos.both(cacheProcess));
				});
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> download(GlobalFsAddress address, long offset, long limit) {
		if (!searchingForDownload.add(address)) {
			return Stage.ofException(RECURSIVE_ERROR);
		}
		return getNamespace(address.getPubKey())
				.thenCompose(ns -> ns.getFileSystem(address.getFilesystem())
						.thenCompose(fs -> fs.download(address.getFile(), offset, limit)
								.thenComposeEx((result, e) -> {
									if (e == null) {
										searchingForDownload.remove(address);
										return Stage.of(result);
									}
									return ns.findNodes()
											.thenCompose(nodes ->
													Stages.firstSuccessful(nodes
															.stream()
															.map(node -> download(node, fs, address, offset, limit))))
											.thenRunEx(() -> searchingForDownload.remove(address));
								})));
	}
}
