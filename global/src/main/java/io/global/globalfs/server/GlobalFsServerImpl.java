package io.global.globalfs.server;

import io.datakernel.async.Stage;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.globalfs.api.GlobalFsDiscoveryService;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsServer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class GlobalFsServerImpl implements GlobalFsServer {
	private final RawServerId myServerId;
	private final GlobalFsDiscoveryService discoveryService;

	public final FsClientFactory fsClientFactory;
	public final CheckpointStorageFactory checkpointStorageFactory;

	private final GlobalFsServerFactory rawServerFactory;

	private final Map<PubKey, GlobalFsServer_PubKey> pubKeyMap = new HashMap<>();

	private GlobalFsServerImpl(RawServerId myServerId,
			GlobalFsDiscoveryService discoveryService, FsClientFactory fsClientFactory, CheckpointStorageFactory checkpointStorageFactory, GlobalFsServerFactory rawServerFactory) {
		this.myServerId = myServerId;
		this.discoveryService = discoveryService;
		this.fsClientFactory = fsClientFactory;
		this.checkpointStorageFactory = checkpointStorageFactory;
		this.rawServerFactory = rawServerFactory;
	}

	private GlobalFsServer_PubKey ensurePubKey(GlobalFsName name) {
		return pubKeyMap.computeIfAbsent(name.getPubKey(),
				k -> new GlobalFsServer_PubKey(this, name.getPubKey()));
	}

	private GlobalFsServer_FileSystem ensureFileSystem(GlobalFsName name) {
		return ensurePubKey(name).ensureFileSystem(name);
	}

	@Override
	public Stage<Set<String>> getFsNames(PubKey pubKey) {
		return null;
	}

	@Override
	public Stage<FilesWithSize> list(GlobalFsName id, long revisionId) {
		return null;
	}

	@Override
	public Stage<List<FileMetadata>> list(GlobalFsName id, String glob) {
		return null;
	}

	@Override
	public Stage<Void> delete(GlobalFsName name, String glob) {
		return null;
	}

	@Override
	public Stage<Set<String>> copy(GlobalFsName name, Map<String, String> changes) {
		return null;
	}

	@Override
	public Stage<Set<String>> move(GlobalFsName name, Map<String, String> changes) {
		return null;
	}

	@Override
	public Stage<StreamProducerWithResult<DataFrame, Void>> download(GlobalFsName id, String filename, long offset, long limit) {
		return null;
	}

	@Override
	public Stage<StreamConsumerWithResult<DataFrame, Void>> upload(GlobalFsName id, String filename, long offset) {
		return null;
	}

	public GlobalFsDiscoveryService getDiscoveryService() {
		return discoveryService;
	}

	public GlobalFsServerFactory getRawServerFactory() {
		return rawServerFactory;
	}
}
