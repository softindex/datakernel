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

package io.global.ot.demo.client;

import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.ot.OTCommit;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.demo.operations.Operation;
import io.global.ot.http.GlobalOTNodeHttpClient;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encode;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.launchers.GlobalConfigConverters.ofRawServerId;
import static io.global.ot.demo.util.Utils.LIST_DIFFS_CODEC;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class Bootstrap implements EventloopService {
	private final Eventloop eventloop;
	private final Config config;
	private final DiscoveryService discoveryService;
	private final GlobalOTNode intermediateServer;
	private final RawServerId masterId;
	private PrivKey privateKey;
	private PubKey publicKey;
	private SimKey simKey;
	private RepoID repositoryId;
	private MyRepositoryId<Operation> myRepositoryId;

	public Bootstrap(Eventloop eventloop, Config config) {
		IAsyncHttpClient httpClient = AsyncHttpClient.create(eventloop);
		this.config = config;
		this.eventloop = eventloop;
		this.discoveryService = HttpDiscoveryService.create(config.get(ofInetSocketAddress(), "discovery.address"), httpClient);
		this.intermediateServer = GlobalOTNodeHttpClient.create(httpClient, config.get("node.server.id"));
		this.masterId = config.get(ofRawServerId(), "master.server.id");
		initializeCredentials();
	}

	public GlobalOTNode getIntermediateServer() {
		return intermediateServer;
	}

	public MyRepositoryId<Operation> getMyRepositoryId() {
		return myRepositoryId;
	}

	public SimKey getSimKey() {
		return simKey;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public Promise<Void> start() {
		return initializeDiscoveryService()
				.thenCompose($ -> initializeRootCommit());
	}

	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	private void initializeCredentials() {
		KeyPair keys = KeyPair.generate();
		privateKey = keys.getPrivKey();
		publicKey = keys.getPubKey();
		simKey = SimKey.generate();
		repositoryId = RepoID.of(publicKey, config.get("repository.name"));
		myRepositoryId = new MyRepositoryId<>(repositoryId, privateKey,
				list -> encode(LIST_DIFFS_CODEC, list).asArray(),
				bytes -> decode(LIST_DIFFS_CODEC, bytes));
	}

	private Promise<Void> initializeDiscoveryService() {
		AnnounceData announceData = new AnnounceData(System.currentTimeMillis(), set(masterId));
		SignedData<AnnounceData> signedData = SignedData.sign(REGISTRY.get(AnnounceData.class), announceData, privateKey);
		return discoveryService.announce(publicKey, signedData);
	}

	private Promise<Void> initializeRootCommit() {
		OTDriver driver = new OTDriver(intermediateServer, emptyList(), repositoryId);
		driver.changeCurrentSimKey(simKey);
		OTCommit<CommitId, Operation> rootCommit = driver.createCommit(myRepositoryId, emptyMap(), 1);
		return driver.push(myRepositoryId, rootCommit)
				.thenCompose($ -> driver.saveSnapshot(myRepositoryId, rootCommit.getId(), emptyList()));
	}

}
