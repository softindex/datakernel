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

package io.datakernel.launchers.globalfs;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.remotefs.FsClient;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.api.NodeClientFactory;
import io.global.fs.http.HttpDiscoveryService;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.LocalGlobalFsNode;

import java.util.HashSet;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.launchers.Initializers.ofEventloopTaskScheduler;
import static io.datakernel.launchers.globalfs.GlobalFsConfigConverters.ofPubKey;
import static io.global.fs.local.LocalGlobalFsNode.DEFAULT_LATENCY_MARGIN;

public class HttpGlobalFsNodeModule extends AbstractModule {

	private HttpGlobalFsNodeModule() {
	}

	public static HttpGlobalFsNodeModule create() {
		return new HttpGlobalFsNodeModule();
	}

	@Provides
	@Singleton
	DiscoveryService provide(Config config, AsyncHttpClient client) {
		return HttpDiscoveryService.create(config.get(ofInetSocketAddress(), "globalfs.discoveryService"), client);
	}

	@Provides
	@Singleton
	NodeClientFactory provide(AsyncHttpClient httpClient) {
		return serverId -> new HttpGlobalFsNode(httpClient, serverId.getInetSocketAddress());
	}

	@Provides
	@Singleton
	GlobalFsNode provide(Config config, DiscoveryService discoveryService, NodeClientFactory nodeClientFactory, FsClient storage) {
		RawServerId id = new RawServerId(config.get(ofInetSocketAddress(), "globalfs.http.listenAddresses"));
		return LocalGlobalFsNode.create(id, discoveryService, nodeClientFactory, storage)
				.withManagedPubKeys(new HashSet<>(config.get(ofList(ofPubKey()), "globalfs.managedRepos")))
				.withDownloadCaching(config.get(ofBoolean(), "globalfs.caching.download", true))
				.withUploadCaching(config.get(ofBoolean(), "globalfs.caching.upload", false))
				.withLatencyMargin(config.get(ofDuration(), "globalfs.fetching.latencyMargin", DEFAULT_LATENCY_MARGIN));
	}

	@Provides
	@Singleton
	EventloopTaskScheduler provide(Config config, Eventloop eventloop, GlobalFsNode node) {
		return EventloopTaskScheduler.create(eventloop, ((LocalGlobalFsNode) node)::fetch)
				.initialize(ofEventloopTaskScheduler(config.getChild("globalfs.fetching")));
	}
}
