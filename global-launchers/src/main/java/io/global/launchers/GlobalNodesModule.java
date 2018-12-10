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

package io.global.launchers;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.*;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.util.guice.OptionalDependency;
import io.global.common.api.DiscoveryService;
import io.global.common.api.NodeFactory;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.GlobalFsNodeServlet;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.LocalGlobalFsNode;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.http.GlobalOTNodeHttpClient;
import io.global.ot.http.RawServerServlet;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.stub.CommitStorageStub;

import java.util.concurrent.ExecutorService;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofRawServerId;
import static io.global.launchers.fs.Initializers.ofLocalGlobalFsNode;

public class GlobalNodesModule extends AbstractModule {
	@Provides
	@Singleton
	Eventloop provide(Config config, OptionalDependency<ThrottlingController> maybeThrottlingController) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withInspector));
	}

	@Provides
	@Singleton
	GlobalOTNode provide(Eventloop eventloop, DiscoveryService discoveryService, NodeFactory<GlobalOTNode> factory, Config config) {
		return GlobalOTNodeImpl.create(eventloop, config.get(ofRawServerId(), "ot.serverId"), discoveryService, new CommitStorageStub(), factory);
	}

	@Provides
	@Singleton
	GlobalFsNode provide(Config config, DiscoveryService discoveryService, NodeFactory<GlobalFsNode> factory, FsClient fsClient) {
		return LocalGlobalFsNode.create(config.get(ofRawServerId(), "fs.serverId"), discoveryService, factory, fsClient)
				.initialize(ofLocalGlobalFsNode(config));
	}

	// TODO eduard: add GlobalDbNode

	@Provides
	@Singleton
	DiscoveryService provide(Config config, IAsyncHttpClient client) {
		return HttpDiscoveryService.create(config.get(ofInetSocketAddress(), "discovery.address"), client);
	}

	@Provides
	@Singleton
	IAsyncHttpClient provide(Eventloop eventloop) {
		return AsyncHttpClient.create(eventloop);
	}

	@Provides
	@Singleton
	AsyncHttpServer provide(Eventloop eventloop, AsyncServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	@Singleton
	AsyncServlet provide(RawServerServlet otServlet, GlobalFsNodeServlet fsServlet) {
		return MiddlewareServlet.create()
				.with("/ot", otServlet)
				.with("/fs", fsServlet);
	}

	@Provides
	@Singleton
	RawServerServlet provideRawServerServlet(GlobalOTNode node) {
		return RawServerServlet.create(node);
	}

	@Provides
	@Singleton
	GlobalFsNodeServlet provideGlobalFsServlet(GlobalFsNode node) {
		return GlobalFsNodeServlet.create(node);
	}

	@Provides
	@Singleton
	FsClient provide(Eventloop eventloop, ExecutorService executor, Config config) {
		return LocalFsClient.create(eventloop, executor, config.get(ofPath(), "fs.storage"));
	}

	@Provides
	@Singleton
	NodeFactory<GlobalFsNode> provideFsNodeFactory(IAsyncHttpClient client) {
		return id -> new HttpGlobalFsNode(client, id.getServerIdString());
	}

	@Provides
	@Singleton
	NodeFactory<GlobalOTNode> provideOTNodeFactory(IAsyncHttpClient client) {
		return id -> GlobalOTNodeHttpClient.create(client, id.getServerIdString());
	}

	@Provides
	@Singleton
	public ExecutorService provide(Config config) {
		return getExecutor(config.getChild("fs.executor"));
	}

}
