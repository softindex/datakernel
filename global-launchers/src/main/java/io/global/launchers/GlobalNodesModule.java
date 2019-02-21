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
import com.google.inject.name.Named;
import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.CachedAsyncDnsClient;
import io.datakernel.dns.DnsCache;
import io.datakernel.dns.RemoteAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.*;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.util.guice.OptionalDependency;
import io.global.common.api.DiscoveryService;
import io.global.common.api.NodeFactory;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.db.LocalGlobalDbNode;
import io.global.db.api.GlobalDbNode;
import io.global.db.http.GlobalDbNodeServlet;
import io.global.db.http.HttpGlobalDbNode;
import io.global.db.stub.RuntimeDbStorageStub;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.GlobalFsNodeServlet;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.LocalGlobalFsNode;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.http.GlobalOTNodeHttpClient;
import io.global.ot.http.RawServerServlet;
import io.global.ot.server.CommitStorage;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.stub.CommitStorageStub;

import java.util.concurrent.ExecutorService;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.dns.RemoteAsyncDnsClient.DEFAULT_TIMEOUT;
import static io.datakernel.dns.RemoteAsyncDnsClient.GOOGLE_PUBLIC_DNS;
import static io.datakernel.launchers.initializers.ConfigConverters.ofDnsCache;
import static io.datakernel.launchers.initializers.Initializers.*;
import static io.global.launchers.GlobalConfigConverters.ofRawServerId;
import static io.global.launchers.db.Initializers.ofLocalGlobalDbNode;
import static io.global.launchers.fs.Initializers.ofLocalGlobalFsNode;
import static io.global.launchers.ot.Initializers.ofGlobalOTNodeImpl;

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
	GlobalOTNodeImpl provide(Eventloop eventloop, DiscoveryService discoveryService, NodeFactory<GlobalOTNode> factory, CommitStorage commitStorage, Config config) {
		return GlobalOTNodeImpl.create(eventloop, config.get(ofRawServerId(), "ot.serverId"), discoveryService, commitStorage, factory)
				.initialize(ofGlobalOTNodeImpl(config.getChild("ot")));
	}

	@Provides
	@Singleton
	LocalGlobalFsNode provide(Config config, DiscoveryService discoveryService, NodeFactory<GlobalFsNode> factory, FsClient fsClient) {
		return LocalGlobalFsNode.create(config.get(ofRawServerId(), "fs.serverId"), discoveryService, factory, fsClient)
				.initialize(ofLocalGlobalFsNode(config.getChild("fs")));
	}

	@Provides
	@Singleton
	LocalGlobalDbNode provide(Config config, DiscoveryService discoveryService, NodeFactory<GlobalDbNode> factory) {
		return LocalGlobalDbNode.create(config.get(ofRawServerId(), "db.serverId"), discoveryService, factory, $ -> new RuntimeDbStorageStub())
				.initialize(ofLocalGlobalDbNode(config.getChild("ot")));
	}

	@Provides
	@Singleton
	DiscoveryService provide(Config config, IAsyncHttpClient client) {
		return HttpDiscoveryService.create(config.get(ofInetSocketAddress(), "discovery.address"), client);
	}

	@Provides
	@Singleton
	IAsyncHttpClient provide(Eventloop eventloop, AsyncDnsClient dnsClient) {
		return AsyncHttpClient.create(eventloop)
				.withDnsClient(dnsClient);
	}

	@Provides
	@Singleton
	AsyncDnsClient provideDnsClient(Eventloop eventloop, Config config) {
		RemoteAsyncDnsClient remoteDnsClient = RemoteAsyncDnsClient.create(eventloop)
				.withDnsServerAddress(config.get(ofInetSocketAddress(), "dns.serverAddress", GOOGLE_PUBLIC_DNS))
				.withTimeout(config.get(ofDuration(), "dns.timeout", DEFAULT_TIMEOUT));
		return CachedAsyncDnsClient.create(eventloop, remoteDnsClient, config.get(ofDnsCache(eventloop), "dns.cache", DnsCache.create(eventloop)));
	}

	@Provides
	@Singleton
	AsyncHttpServer provide(Eventloop eventloop, AsyncServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	@Singleton
	AsyncServlet provide(RawServerServlet otServlet, GlobalFsNodeServlet fsServlet, GlobalDbNodeServlet dbServlet) {
		return MiddlewareServlet.create()
				.with("/ot", otServlet)
				.with("/fs", fsServlet)
				.with("/db", dbServlet);
	}

	@Provides
	@Singleton
	RawServerServlet provideRawServerServlet(GlobalOTNodeImpl node) {
		return RawServerServlet.create(node);
	}

	@Provides
	@Singleton
	GlobalFsNodeServlet provideGlobalFsServlet(LocalGlobalFsNode node) {
		return GlobalFsNodeServlet.create(node);
	}

	@Provides
	@Singleton
	GlobalDbNodeServlet provideGlobalDbServlet(LocalGlobalDbNode node) {
		return GlobalDbNodeServlet.create(node);
	}

	@Provides
	@Singleton
	CommitStorage provideCommitStorage() {
		return new CommitStorageStub();
	}

	@Provides
	@Singleton
	FsClient provide(Eventloop eventloop, ExecutorService executor, Config config) {
		return LocalFsClient.create(eventloop, config.get(ofPath(), "fs.storage"));
	}

	@Provides
	@Singleton
	NodeFactory<GlobalFsNode> provideFsNodeFactory(IAsyncHttpClient client) {
		return id -> HttpGlobalFsNode.create(id.getServerIdString(), client);
	}

	@Provides
	@Singleton
	NodeFactory<GlobalOTNode> provideOTNodeFactory(IAsyncHttpClient client) {
		return id -> GlobalOTNodeHttpClient.create(client, id.getServerIdString());
	}

	@Provides
	@Singleton
	NodeFactory<GlobalDbNode> provideDbNodeFactory(IAsyncHttpClient client) {
		return id -> HttpGlobalDbNode.create(id.getServerIdString(), client);
	}

	@Provides
	@Singleton
	public ExecutorService provide(Config config) {
		return getExecutor(config.getChild("fs.executor"));
	}

	// region schedulers
	@Provides
	@Singleton
	@Named("FS push")
	EventloopTaskScheduler provideFsPushScheduler(Eventloop eventloop, LocalGlobalFsNode node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::push)
				.initialize(ofEventloopTaskScheduler(config.getChild("fs.push")));
	}

	@Provides
	@Singleton
	@Named("FS catch up")
	EventloopTaskScheduler provideFsCatchUpScheduler(Eventloop eventloop, LocalGlobalFsNode node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::catchUp)
				.initialize(ofEventloopTaskScheduler(config.getChild("fs.catchUp")));
	}

	@Provides
	@Singleton
	@Named("OT push")
	EventloopTaskScheduler provideOTPushScheduler(Eventloop eventloop, GlobalOTNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::push)
				.initialize(ofEventloopTaskScheduler(config.getChild("ot.push")));
	}

	@Provides
	@Singleton
	@Named("OT catch up")
	EventloopTaskScheduler provideOTCatchUpScheduler(Eventloop eventloop, GlobalOTNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::catchUp)
				.initialize(ofEventloopTaskScheduler(config.getChild("ot.catchUp")));
	}

	@Provides
	@Singleton
	@Named("OT update")
	EventloopTaskScheduler provideOTUpdateScheduler(Eventloop eventloop, GlobalOTNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::update)
				.initialize(ofEventloopTaskScheduler(config.getChild("ot.update")));
	}

	@Provides
	@Singleton
	@Named("DB push")
	EventloopTaskScheduler provideDbPushScheduler(Eventloop eventloop, LocalGlobalDbNode node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::push)
				.initialize(ofEventloopTaskScheduler(config.getChild("db.push")));
	}

	@Provides
	@Singleton
	@Named("DB catch up")
	EventloopTaskScheduler provideDbCatchUpScheduler(Eventloop eventloop, LocalGlobalDbNode node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::catchUp)
				.initialize(ofEventloopTaskScheduler(config.getChild("db.catchUp")));
	}
	//endregion

}
