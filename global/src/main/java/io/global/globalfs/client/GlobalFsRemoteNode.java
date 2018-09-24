package io.global.globalfs.client;

import io.datakernel.async.Stage;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.serial.SerialSupplier;
import io.global.common.PubKey;
import io.global.globalfs.api.*;

import java.util.Set;

public class GlobalFsRemoteNode implements GlobalFsNode {

	private final AsyncHttpClient client;
	private final String host;

	public GlobalFsRemoteNode(AsyncHttpClient client, String host) {
		this.client = client;
		this.host = host;
	}

	@Override
	public Stage<Set<String>> getFilesystemNames(PubKey pubKey) {
		throw new UnsupportedOperationException("GlobalFsRemoteNode#getFilesystemNames is not implemented yet");
	}

	@Override
	public Stage<GlobalFsNamespace> getNamespace(PubKey key) {
		throw new UnsupportedOperationException("GlobalFsRemoteNode#getNamespace is not implemented yet");
	}

	@Override
	public Stage<GlobalFsFileSystem> getFileSystem(GlobalFsName name) {
		throw new UnsupportedOperationException("GlobalFsRemoteNode#getFileSystem is not implemented yet");
	}

	@Override
	public Stage<SerialSupplier<DataFrame>> download(GlobalFsAddress file, long offset, long limit) {
		throw new UnsupportedOperationException("GlobalFsRemoteNode#download is not implemented yet");
	}

	@Override
	public Settings getSettings() {
		throw new UnsupportedOperationException("GlobalFsRemoteNode#getSettings is not implemented yet");
	}
}
