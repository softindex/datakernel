package io.global.globalsync.http;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.http.HttpServerLauncher;
import io.global.common.PubKey;
import io.global.globalsync.api.RawDiscoveryService;
import io.global.globalsync.api.RawDiscoveryServiceStub;
import io.global.globalsync.api.RawServer;
import io.global.globalsync.api.RepositoryName;
import io.global.globalsync.server.CommitStorage;
import io.global.globalsync.server.RawServerImpl;
import io.global.globalsync.server.RawServer_PubKey;
import io.global.globalsync.server.RawServer_Repository;
import io.global.globalsync.stub.CommitStorageStub;

import java.time.Duration;
import java.util.Collection;

import static java.util.Arrays.asList;

public final class CubeHttpServerLauncher extends HttpServerLauncher {
	@Override
	protected final Collection<Module> getBusinessLogicModules() {
		return asList(new MyAbstractModule());
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new CubeHttpServerLauncher();
		launcher.launch(true, args);
	}

	private static class MyAbstractModule extends AbstractModule {
		@Provides
		@Singleton
		AsyncServlet asyncServlet(RawServer server) {
			return RawServerServlet.create(server);
		}

		@Provides
		@Singleton
		RawServer server(Eventloop eventloop,
				RawDiscoveryService rawDiscoveryService,
				CommitStorage commitStorage) {
			return new RawServerImpl(eventloop,
					rawDiscoveryService,
					commitStorage,
					new RawServerImpl.Settings() {
						@Override
						public RawServer_PubKey.Settings getPubKeySettings(PubKey pubKey) {
							return new RawServer_PubKey.Settings() {
								@Override
								public Duration getLatencyMargin() {
									return null;
								}

								@Override
								public RawServer_Repository.Settings getRepositorySettings(RepositoryName repositoryId) {
									return null;
								}
							};
						}
					});
		}

		@Provides
		@Singleton
		RawDiscoveryService discoveryService() {
			return new RawDiscoveryServiceStub();
		}

		@Provides
		@Singleton
		CommitStorage commitStorage() {
			return new CommitStorageStub();
		}
	}
}
