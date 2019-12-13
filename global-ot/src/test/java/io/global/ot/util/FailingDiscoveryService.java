package io.global.ot.util;

import io.datakernel.promise.Promise;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import org.jetbrains.annotations.Nullable;

import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FailingDiscoveryService implements DiscoveryService {
	public static final Exception ERROR = new ConnectException("Connection failed");

	@Override
	public Promise<Void> announce(PubKey space, SignedData<AnnounceData> announceData) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<@Nullable SignedData<AnnounceData>> find(PubKey space) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<Map<PubKey, Set<RawServerId>>> findAll() {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<Void> shareKey(PubKey receiver, SignedData<SharedSimKey> simKey) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<SignedData<SharedSimKey>> getSharedKey(PubKey receiver, Hash hash) {
		return Promise.ofException(ERROR);
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> getSharedKeys(PubKey receiver) {
		return Promise.ofException(ERROR);
	}
}
