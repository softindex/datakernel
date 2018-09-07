package io.global.globalfs.server;

import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.exception.StacklessException;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class GlobalFsClientLocalImpl implements GlobalFsClient {
	private final Map<PubKey, PublicKeyFsGroup> publicKeyGroups = new HashMap<>();

	private final RawServerId id;
	private final DiscoveryService discoveryService;
	private final RawClientFactory clientFactory;
	private final FileSystemFactory fileSystemFactory;

	public GlobalFsClientLocalImpl(
			RawServerId id,
			DiscoveryService discoveryService,
			RawClientFactory clientFactory,
			FileSystemFactory fileSystemFactory
	) {
		this.id = id;
		this.discoveryService = discoveryService;
		this.clientFactory = clientFactory;
		this.fileSystemFactory = fileSystemFactory;
	}

	private PublicKeyFsGroup getPublicKeyFsGroup(PubKey pubKey) {
		return publicKeyGroups.computeIfAbsent(pubKey, $ -> new PublicKeyFsGroup(this, pubKey));
	}

	private GlobalFsFileSystem getFileSystem(GlobalFsName name) {
		return getPublicKeyFsGroup(name.getPubKey()).getFileSystem(name.getFsName());
	}

	// region getters
	public RawServerId getId() {
		return id;
	}

	public DiscoveryService getDiscoveryService() {
		return discoveryService;
	}

	public RawClientFactory getClientFactory() {
		return clientFactory;
	}

	public FileSystemFactory getFileSystemFactory() {
		return fileSystemFactory;
	}
	// endregion

	@Override
	public Stage<Set<String>> getFsNames(PubKey pubKey) {
		return Stage.of(publicKeyGroups.get(pubKey).getFsNames());
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> download(GlobalFsName name, String filename, long offset, long limit) {
		PublicKeyFsGroup group = getPublicKeyFsGroup(name.getPubKey());
		return group.getFileSystem(name.getFsName()).download(filename, offset, limit)
				.thenComposeEx((result, e) -> {
					if (e == null) {
						return Stage.of(result);
					}
					return group.getServers()
							.thenCompose(servers ->
									Stages.firstSuccessful(servers
											.stream()
											.map(server ->
													server.list(name, filename)
															.thenCompose(res -> {
																if (res.size() != 1) {
																	return Stage.ofException(new StacklessException("no such file"));
																}
																return server.downloadFromThis(name, filename, offset, limit);
															}))));
				});
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> downloadFromThis(GlobalFsName name, String filename, long offset, long limit) {
		return getFileSystem(name).download(filename, offset, limit);
	}

	@Override
	public Stage<SerialConsumer<DataFrame>> upload(GlobalFsName name, String filename, long offset) {
		return getFileSystem(name).upload(filename, offset);
	}

	@Override
	public Stage<Revision> list(GlobalFsName name, long revisionId) {
		return getFileSystem(name).list(revisionId);
	}

	@Override
	public Stage<List<FileMetadata>> list(GlobalFsName name, String glob) {
		return getFileSystem(name).list(glob);
	}

	@Override
	public Stage<Void> delete(GlobalFsName name, String glob) {
		return getFileSystem(name).delete(glob);
	}

	@Override
	public Stage<Set<String>> copy(GlobalFsName name, Map<String, String> changes) {
		return getFileSystem(name).copy(changes);
	}

	@Override
	public Stage<Set<String>> move(GlobalFsName name, Map<String, String> changes) {
		return getFileSystem(name).move(changes);
	}
}
