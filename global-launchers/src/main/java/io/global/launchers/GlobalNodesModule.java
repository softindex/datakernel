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
import io.global.kv.GlobalKvNodeImpl;
import io.global.kv.RocksDbStorageFactory;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.StorageFactory;
import io.global.kv.http.GlobalKvNodeServlet;
import io.global.kv.http.HttpGlobalKvNode;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.http.HttpGlobalOTNode;
import io.global.ot.http.RawServerServlet;
import io.global.ot.server.CommitStorage;
import io.global.ot.server.CommitStorageRocksDb;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.server.ValidatingGlobalOTNode;
import io.global.pm.GlobalPmNodeImpl;
import io.global.pm.RocksDbMessageStorage;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.api.MessageStorage;
import io.global.pm.http.GlobalPmNodeServlet;
import io.global.pm.http.HttpGlobalPmNode;

import java.util.concurrent.Executor;
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
		bind(GlobalKvNode.class).to(GlobalKvNodeImpl.class);
		bind(GlobalOTNode.class).to(GlobalOTNodeImpl.class);
		bind(GlobalPmNode.class).to(GlobalPmNodeImpl.class);
	}

	@Provides
	Eventloop eventloop(Config config, @Optional ThrottlingController throttlingController) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(eventloop -> eventloop.withInspector(throttlingController));
	}

	@Provides
	GlobalOTNodeImpl globalOTNode(Eventloop eventloop, RawServerId serverId, DiscoveryService discoveryService, Function<RawServerId, GlobalOTNode> factory, CommitStorage commitStorage, Config config) {
		return GlobalOTNodeImpl.create(eventloop, serverId, discoveryService, commitStorage, factory)
				.initialize(ofAbstractGlobalNode(config.getChild("ot")))
				.initialize(ofGlobalOTNodeImpl(config.getChild("ot")));
	}

	@Provides
	GlobalFsNodeImpl globalFsNode(Config config, RawServerId serverId, DiscoveryService discoveryService, Function<RawServerId, GlobalFsNode> factory,
			@Named("FS") FsClient fsClient) {
		return GlobalFsNodeImpl.create(serverId, discoveryService, factory, fsClient)
				.initialize(ofAbstractGlobalNode(config.getChild("fs")))
				.initialize(ofLocalGlobalFsNode(config.getChild("fs")));
	}

	@Provides
	GlobalKvNodeImpl globalKvNode(Config config, RawServerId serverId, DiscoveryService discoveryService, Function<RawServerId, GlobalKvNode> factory,
			StorageFactory storageFactory) {
		return GlobalKvNodeImpl.create(serverId, discoveryService, factory, storageFactory)
				.initialize(ofAbstractGlobalNode(config.getChild("kv")));
	}

	@Provides
	GlobalPmNodeImpl globalPmNode(Config config, RawServerId serverId, DiscoveryService discoveryService, Function<RawServerId, GlobalPmNode> factory, MessageStorage storage) {
		return GlobalPmNodeImpl.create(serverId, discoveryService, factory, storage)
				.initialize(ofAbstractGlobalNode(config.getChild("pm")));
	}

	@Provides
	GlobalFsDriver globalFsDriver(GlobalFsNode node, Config config) {
		return GlobalFsDriver.create(node, CheckpointPosStrategy.of(config.get(ofLong(), "app.checkpointOffset", 16384L)));
	}

	@Provides
	<K, V> GlobalKvDriver<K, V> globalKvDriver(GlobalKvNode node, StructuredCodec<K> keyCodec, StructuredCodec<V> valueCodec) {
		return GlobalKvDriver.create(node, keyCodec, valueCodec.nullable());
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
	AsyncServlet servlet(RawServerServlet otServlet, @Named("fs") AsyncServlet fsServlet,
			@Named("kv") AsyncServlet kvServlet, @Named("pm") AsyncServlet pmServlet) {
		return RoutingServlet.create()
				.map("/ot/*", otServlet)
				.map("/fs/*", fsServlet)
				.map("/kv/*", kvServlet)
				.map("/pm/*", pmServlet);
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
	@Named("pm")
	AsyncServlet pmServlet(GlobalPmNode node) {
		return GlobalPmNodeServlet.create(node);
	}

	@Provides
	CommitStorage commitStorage(Eventloop eventloop, Config config, Executor executor) {
		return CommitStorageRocksDb.create(executor, eventloop, config.get("ot.storage"));
	}

	@Provides
	StorageFactory kvStorageFactory(Eventloop eventloop, Config config, Executor executor) {
		return RocksDbStorageFactory.create(eventloop, executor, config.get("kv.storage"));
	}

	@Provides
	@Named("FS")
	FsClient fsClient(Eventloop eventloop, Config config) {
		return LocalFsClient.create(eventloop, config.get(ofPath(), "fs.storage"))
				.withRevisions();
	}

	@Provides
	MessageStorage messageStorage(Eventloop eventloop, Executor executor, Config config) {
		return RocksDbMessageStorage.create(eventloop, executor, config.get("pm.storage"));
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

	@Provides
	Function<RawServerId, GlobalPmNode> pmNodeFactory(IAsyncHttpClient client) {
		return id -> HttpGlobalPmNode.create(id.getServerIdString(), client);
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
	@Named("KV push")
	EventloopTaskScheduler kvPushScheduler(Eventloop eventloop, GlobalKvNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::push)
				.initialize(ofEventloopTaskScheduler(config.getChild("kv.push")));
	}

	@Provides
	@Named("KV catch up")
	EventloopTaskScheduler kvCatchUpScheduler(Eventloop eventloop, GlobalKvNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::catchUp)
				.initialize(ofEventloopTaskScheduler(config.getChild("kv.catchUp")));
	}

	@Provides
	@Named("PM push")
	EventloopTaskScheduler pmPushScheduler(Eventloop eventloop, GlobalPmNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::push)
				.initialize(ofEventloopTaskScheduler(config.getChild("pm.push")));
	}

	@Provides
	@Named("PM catch up")
	EventloopTaskScheduler pmCatchUpScheduler(Eventloop eventloop, GlobalPmNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::catchUp)
				.initialize(ofEventloopTaskScheduler(config.getChild("pm.catchUp")));
	}

	@Provides
	@Named("OT push")
	EventloopTaskScheduler otPushScheduler(Eventloop eventloop, GlobalOTNodeImpl node, Config config) {
		return EventloopTaskScheduler.create(eventloop, node::push)
				.initialize(ofEventloopTaskScheduler(config.getChild("ot.push")));
	}
	//endregion
}
