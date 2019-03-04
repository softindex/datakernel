package io.global.ot.util;

import io.datakernel.async.Promise;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import org.jetbrains.annotations.Nullable;

import java.net.ConnectException;
import java.util.List;

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
