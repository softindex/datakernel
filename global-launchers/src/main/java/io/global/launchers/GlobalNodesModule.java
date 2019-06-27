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

import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.dns.CachedAsyncDnsClient;
import io.datakernel.dns.DnsCache;
import io.datakernel.dns.RemoteAsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.http.*;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.HttpDiscoveryService;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.GlobalFsNodeServlet;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.GlobalFsDriver;
import io.global.fs.local.GlobalFsNodeImpl;
import io.global.kv.GlobalKvDriver;
import io.global.kv.LocalGlobalKvNode;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.http.GlobalKvNodeServlet;
import io.global.kv.http.HttpGlobalKvNode;
import io.global.kv.stub.RuntimeKvStorageStub;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.http.HttpGlobalOTNode;
import io.global.ot.http.RawServerServlet;
import io.global.ot.server.CommitStorage;
import io.global.ot.server.CommitStorageRocksDb;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.server.ValidatingGlobalOTNode;

import java.util.function.Function;

import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.dns.RemoteAsyncDnsClient.DEFAULT_TIMEOUT;
import static io.datakernel.dns.RemoteAsyncDnsClient.GOOGLE_PUBLIC_DNS;
import static io.datakernel.launchers.initializers.ConfigConverters.ofDnsCache;
import static io.datakernel.launchers.initializers.Initializers.*;
import static io.global.launchers.GlobalConfigConverters.ofRawServerId;
import static io.global.launchers.Initializers.ofAbstractGlobalNode;
import static io.global.launchers.fs.Initializers.ofLocalGlobalFsNode;
import static io.global.launchers.ot.Initializers.ofGlobalOTNodeImpl;

public class GlobalNodesModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(GlobalFsNode.class).to(GlobalFsNodeImpl.class);
		bind(GlobalKvNode.class).to(LocalGlobalKvNode.class);
	}

	@Provides
	Eventloop eventloop(Config config, @Optional ThrottlingController throttlingController) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(eventloop -> eventloop.withInspector(throttlingController));
	}

	@Provides
	GlobalOTNode globalOTNode(Eventloop eventloop, RawServerId serverId, DiscoveryService discoveryService, Function<RawServerId, GlobalOTNode> factory, CommitStorage commitStorage, Config config) {
		return GlobalOTNodeImpl.create(eventloop, serverId, discoveryService, commitStorage, factory)
				.initialize(ofAbstractGlobalNode(config.getChild("ot")))
				.initialize(ofGlobalOTNodeImpl(config.getChild("ot")));
	}

	@Provides
	GlobalFsNodeImpl globalFsNode(Config config, RawServerId serverId, DiscoveryService discoveryService, Function<RawServerId, GlobalFsNode> factory, FsClient fsClient) {
		return GlobalFsNodeImpl.create(serverId, discoveryService, factory, fsClient)
				.initialize(ofAbstractGlobalNode(config.getChild("fs")))
				.initialize(ofLocalGlobalFsNode(config.getChild("fs")));
	}

	@Provides
	LocalGlobalKvNode localGlobalKvNode(Config config, RawServerId serverId, DiscoveryService discoveryService, Function<RawServerId, GlobalKvNode> factory) {
		return LocalGlobalKvNode.create(serverId, discoveryService, factory, ($1, $2) -> new RuntimeKvStorageStub())
				.initialize(ofAbstractGlobalNode(config.getChild("kv")));
	}

	@Provides
	GlobalFsDriver globalFsDriver(GlobalFsNode node, Config config) {
		return GlobalFsDriver.create(node, CheckpointPosStrategy.of(config.get(ofLong(), "app.checkpointOffset", 16384L)));
	}

	@Provides
	<K, V> GlobalKvDriver<K, V> globalKvDriver(GlobalKvNode node, StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
		return GlobalKvDriver.create(node, keyCodec, valueCodec);
	}

	@Provides
	<T> StructuredCodec<T> codecProvider(Key<T> reifiedT, CodecFactory codecs) {
		return codecs.get(reifiedT.getType());
	}

	@Provides
	RawServerId rawServerId(Config config) {
		return config.get(ofRawServerId(), "node.serverId");
	}

	@Provides
	DiscoveryService discoveryService(Config config, IAsyncHttpClient client) {
		return HttpDiscoveryService.create(config.get(ofInetSocketAddress(), "discovery.address"), client);
	}

	@Provides
	IAsyncHttpClient asyncHttpClient(Eventloop eventloop, AsyncDnsClient dnsClient) {
		return AsyncHttpClient.create(eventloop)
				.withDnsClient(dnsClient);
	}

	@Provides
	AsyncDnsClient asyncDnsClient(Eventloop eventloop, Config config) {
		RemoteAsyncDnsClient remoteDnsClient = RemoteAsyncDnsClient.create(eventloop)
				.withDnsServerAddress(config.get(ofInetSocketAddress(), "dns.serverAddress", GOOGLE_PUBLIC_DNS))
				.withTimeout(config.get(ofDuration(), "dns.timeout", DEFAULT_TIMEOUT));
		return CachedAsyncDnsClient.create(eventloop, remoteDnsClient, config.get(ofDnsCache(eventloop), "dns.cache", DnsCache.create(eventloop)));
	}

	@Provides
	@Named("Nodes")
	AsyncHttpServer asyncHttpServer(Eventloop eventloop, @Named("Nodes") AsyncServlet servlet, RawServerServlet rawServerServlet, Config config) {
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));

		rawServerServlet.setCloseNotification(server.getCloseNotification());

		return server;
	}

	@Provides
	@Named("Nodes")
	AsyncServlet servlet(RawServerServlet otServlet, @Named("fs") AsyncServlet fsServlet, @Named("kv") AsyncServlet kvServlet) {
		return RoutingServlet.create()
				.with("/ot/*", otServlet)
				.with("/fs/*", fsServlet)
				.with("/kv/*", kvServlet);
	}

	@Provides
	RawServerServlet rawServerServlet(GlobalOTNode node) {
		return RawServerServlet.create(ValidatingGlobalOTNode.create(node));
	}

	@Provides
	@Named("fs")
	AsyncServlet fsServlet(GlobalFsNode node) {
		return GlobalFsNodeServlet.create(node);
	}

	@Provides
	@Named("kv")
	AsyncServlet kvServlet(GlobalKvNode node) {
		return GlobalKvNodeServlet.create(node);
	}

	@Provides
	CommitStorage commitStorage(Eventloop eventloop, Config config) {
		return CommitStorageRocksDb.create(eventloop, config.get("ot.storage"));
	}

	@Provides
	FsClient fsClient(Eventloop eventloop, Config config) {
		return LocalFsClient.create(eventloop, config.get(ofPath(), "fs.storage"))
				.withRevisions();
	}

	@Provides
	Function<RawServerId, GlobalFsNode> fsNodeFactory(IAsyncHttpClient client) {
		return id -> HttpGlobalFsNode.create(id.getServerIdString(), client);
	}

	@Provides
	Function<RawServerId, GlobalOTNode> otNodeFactory(IAsyncHttpClient client) {
		return id -> ValidatingGlobalOTNode.create(HttpGlobalOTNode.create(id.getServerIdString(), client));
	}

	@Provides
	Function<RawServerId, GlobalKvNode> kvNodeFactory(IAsyncHttpClient client) {
		return id -> HttpGlobalKvNode.create(id.getServerIdString(), client);
	}

	// region schedulers
	@Provides
	@Named("FS push")
	EventloopTaskScheduler fsPushScheduler(Eventloop eventloop, GlobalFsNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::push)
				.initialize(ofEventloopTaskScheduler(config.getChild("fs.push")));
	}

	@Provides
	@Named("FS catch up")
	EventloopTaskScheduler fsCatchUpScheduler(Eventloop eventloop, GlobalFsNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::catchUp)
				.initialize(ofEventloopTaskScheduler(config.getChild("fs.catchUp")));
	}

	@Provides
	@Named("DB push")
	EventloopTaskScheduler dbPushScheduler(Eventloop eventloop, LocalGlobalKvNode node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::push)
				.initialize(ofEventloopTaskScheduler(config.getChild("kv.push")));
	}

	@Provides
	@Named("DB catch up")
	EventloopTaskScheduler dbCatchUpScheduler(Eventloop eventloop, LocalGlobalKvNode node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::catchUp)
				.initialize(ofEventloopTaskScheduler(config.getChild("kv.catchUp")));
	}
	//endregion

}
