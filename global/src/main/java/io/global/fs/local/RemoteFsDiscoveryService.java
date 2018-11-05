/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.global.fs.local;

import io.datakernel.async.Promise;
import io.datakernel.remotefs.FsClient;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;

import java.util.Optional;

public final class RemoteFsDiscoveryService implements DiscoveryService {
	private final FsClient fsClient;

	public RemoteFsDiscoveryService(FsClient fsClient) {
		this.fsClient = fsClient;
	}

	@Override
	public Promise<Void> announce(PubKey space, SignedData<AnnounceData> announceData) {
		throw new UnsupportedOperationException("RemoteFsDiscoveryService#announce is not implemented yet");
	}

	@Override
	public Promise<SignedData<AnnounceData>> find(PubKey space) {
		throw new UnsupportedOperationException("RemoteFsDiscoveryService#find is not implemented yet");
	}

	@Override
	public Promise<Void> shareKey(PubKey owner, SignedData<SharedSimKey> simKey) {
		throw new UnsupportedOperationException("RemoteFsDiscoveryService#shareKey is not implemented yet");
	}

	@Override
	public Promise<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey owner, PubKey receiver, Hash hash) {
		throw new UnsupportedOperationException("RemoteFsDiscoveryService#getSharedKey is not implemented yet");
	}
}
