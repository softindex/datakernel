package io.datakernel.boot.cube;

import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.boot.http.HttpServerLauncher;
import io.datakernel.config.Config;
import io.datakernel.cube.Cube;
import io.datakernel.cube.http.ReportingServiceServlet;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.ot.OTStateManager;
import io.datakernel.util.guice.SimpleModule;

import javax.inject.Named;
import java.util.Collection;
import java.util.Collections;

import static com.google.inject.util.Modules.combine;
import static io.datakernel.config.ConfigConverters.ofLong;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;

public abstract class CubeHttpServerLauncher extends HttpServerLauncher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String CUBE_MODULE_PROP = "cubeModule";

	@Override
	protected final Collection<Module> getBusinessLogicModules() {
		return asList(
				combine(getCubeModules()),
				new SimpleModule() {
					@Provides
					@Singleton
					@Named("CubePullScheduler")
					EventloopTaskScheduler pullScheduler(Config config, Eventloop eventloop,
					                                     OTStateManager<Integer, LogDiff<CubeDiff>> cubeStateManager) {
						return EventloopTaskScheduler.create(eventloop, pullOrCheckoutTask(cubeStateManager))
								.withPeriod(config.get(ofLong(), "CubeHttpServer.metadataRefreshPeriodMillis", 10_000L));
					}

					@Provides
					@Singleton
					AsyncServlet asyncServlet(Eventloop eventloop, Cube cube) {
						return ReportingServiceServlet.createRootServlet(eventloop, cube);
					}
				});
	}

	private AsyncCallable<Void> pullOrCheckoutTask(OTStateManager<Integer, LogDiff<CubeDiff>> cubeStateManager) {
		return () -> cubeStateManager.pull()
				.handle((revisionId, throwable, next) -> {
					if (throwable == null) {
						next.complete(null);
					} else {
						cubeStateManager.checkout()
								.toVoid()
								.whenComplete(next::complete);
					}
				});
	}

	protected abstract Collection<Module> getCubeModules();

	public static void main(String[] args) throws Exception {
		String cubeModuleName = System.getProperty(CUBE_MODULE_PROP);
		Module cubeModule = cubeModuleName != null ?
				(Module) Class.forName(cubeModuleName).newInstance() :
				new ExampleCubeModule();

		Launcher launcher = new CubeHttpServerLauncher() {
			@Override
			protected Collection<Module> getCubeModules() {
				return Collections.singletonList(cubeModule);
			}
		};
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}