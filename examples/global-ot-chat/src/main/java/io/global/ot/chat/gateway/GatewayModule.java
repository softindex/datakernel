package io.global.ot.chat.gateway;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRepository;
import io.global.common.PrivKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RepoID;
import io.global.ot.chat.common.Gateway;
import io.global.ot.chat.common.GatewayImpl;
import io.global.ot.chat.common.Operation;
import io.global.ot.chat.http.GatewayServlet;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.http.GlobalOTNodeHttpClient;

import static io.datakernel.launchers.initializers.Initializers.*;
import static io.global.launchers.GlobalConfigConverters.ofPrivKey;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.ot.GlobalOTConfigConverters.ofRepoID;
import static io.global.ot.chat.operations.ChatOTSystem.createOTSystem;
import static java.util.Collections.*;

public class GatewayModule extends AbstractModule {

	@Provides
	@Singleton
	Eventloop provideEventloop(Config config) {
		return Eventloop.create()
				.initialize(ofEventloop(config));
	}

	@Provides
	@Singleton
	AsyncHttpServer provideServer(Eventloop eventloop, AsyncServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofAbstractServer(config.getChild("http")));
	}

	@Provides
	@Singleton
	AsyncServlet provideServlet(GatewayServlet<Operation> gatewayServlet) {
		return MiddlewareServlet.create()
				.with("/api", gatewayServlet);
	}

	@Provides
	@Singleton
	GatewayServlet<Operation> provideGatewayServlet(Gateway<Operation> gateway, StructuredCodec<Operation> operationCodec) {
		return GatewayServlet.create(gateway, operationCodec);
	}

	@Provides
	@Singleton
	Gateway<Operation> provideGateway(OTAlgorithms<CommitId, Operation> algorithms) {
		return GatewayImpl.create(algorithms);
	}

	@Provides
	@Singleton
	OTAlgorithms<CommitId, Operation> provideAlgorithms(Eventloop eventloop, OTRepository<CommitId, Operation> repository) {
		return OTAlgorithms.create(eventloop, createOTSystem(), repository);
	}

	@Provides
	@Singleton
	OTRepository<CommitId, Operation> provideRepository(Eventloop eventloop, GlobalOTNode node, Config config, StructuredCodec<Operation> operationCodec) {
		RepoID repoID = config.get(ofRepoID(), "credentials.repositoryId");
		PrivKey privKey = config.get(ofPrivKey(), "credentials.privateKey");
		MyRepositoryId<Operation> myRepositoryId = new MyRepositoryId<>(repoID, privKey, operationCodec);
		OTDriver driver = new OTDriver(node, config.get(ofSimKey(), "credentials.simKey"));
		initializeRepo(eventloop, driver, myRepositoryId);
		return new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
	}

	@Provides
	@Singleton
	GlobalOTNode provideGlobalOTNode(Eventloop eventloop, Config config) {
		return GlobalOTNodeHttpClient.create(AsyncHttpClient.create(eventloop), config.get("node.address"));
	}

	@Provides
	@Singleton
	EventloopTaskScheduler provideMergeScheduler(Eventloop eventloop, Config config, Gateway<Operation> gateway) {
		return EventloopTaskScheduler.create(eventloop, ((GatewayImpl<Operation>) gateway)::mergeHeads)
				.initialize(ofEventloopTaskScheduler(config.getChild("gateway.merge")));
	}

	private void initializeRepo(Eventloop eventloop, OTDriver driver, MyRepositoryId<Operation> myRepositoryId) {
		OTCommit<CommitId, Operation> rootCommit = driver.createCommit(myRepositoryId, emptyMap(), 1);
		eventloop.post(() ->
				driver.getHeads(myRepositoryId.getRepositoryId())
						.thenCompose(heads -> !heads.isEmpty() ?
								Promise.complete() :
								driver.push(myRepositoryId, rootCommit)
										.thenCompose($ -> driver.saveSnapshot(myRepositoryId, rootCommit.getId(), emptyList())))
		);
	}
}
