package io.datakernel;

import com.google.gson.TypeAdapter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.datakernel.async.AsyncFunction;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.cube.ot.CubeOT;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.logfs.ot.LogOT;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemoteSql;
import io.datakernel.ot.OTSystem;
import io.datakernel.utils.GraphUtils;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Predicate;

import static io.datakernel.utils.GraphUtils.*;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class CubeGraphExample {

	private static HikariDataSource dataSource(String databasePropertiesPath) throws IOException {
		Properties properties = new Properties();
		properties.load(new InputStreamReader(new BufferedInputStream(new FileInputStream(new File(databasePropertiesPath))), StandardCharsets.UTF_8));
		return new HikariDataSource(new HikariConfig(properties));
	}

	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		final DataSource dataSource = dataSource("cube/test.properties");
		final ExecutorService executor = Executors.newFixedThreadPool(4);
		final TypeAdapter<LogDiff<CubeDiff>> diffAdapter = skipReadAdapter(() -> LogDiff.of(emptyMap(), emptyList()));
		final OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		final OTRemoteSql<LogDiff<CubeDiff>> otRemoteSql = OTRemoteSql.create(executor, dataSource, otSystem, diffAdapter);
		final AsyncFunction<Integer, OTCommit<Integer, LogDiff<CubeDiff>>> loader = createLoader(otRemoteSql);

		final StringBuilder sb = new StringBuilder();
		final CompletableFuture<Void> future = otRemoteSql.getHeads()
				.thenCompose(heads -> visitNodes(loader, dotSqlProcessor(otRemoteSql, sb),
						CubeGraphExample::getParents, visitOnceTill(1), heads))
				.thenCompose($ -> appendMerges(otRemoteSql, sb))
				.toCompletableFuture();

		eventloop.run();
		future.get();

		System.out.println(String.format("digraph g{%n%s%n}", sb));
		executor.shutdown();
	}

	private static <K, D> Set<K> getParents(OTCommit<K, D> otCommit) {
		return otCommit.getParents().keySet();
	}

	private static Predicate<Integer> visitOnceTill(int n) {
		final Predicate<Integer> visitOnceFilter = visitOnceFilter();
		return integer -> integer >= n && visitOnceFilter.test(integer);
	}

	private static <D> AsyncFunction<Integer, OTCommit<Integer, D>> createLoader(OTRemoteSql<D> otRemoteSql) {
		return commitId -> {
			System.out.println("load commit id: " + commitId);
			return GraphUtils.sqlLoaderWithoutCheckpointAndDiffs(otRemoteSql).apply(commitId);
		};
	}
}
