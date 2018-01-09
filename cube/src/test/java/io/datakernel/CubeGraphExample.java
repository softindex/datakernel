package io.datakernel;

import com.google.gson.TypeAdapter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.datakernel.async.AsyncFunction;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.cube.ot.CubeOT;
import io.datakernel.eventloop.Eventloop;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
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
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		DataSource dataSource = dataSource("cube/test.properties");
		ExecutorService executor = Executors.newFixedThreadPool(4);
		TypeAdapter<LogDiff<CubeDiff>> diffAdapter = skipReadAdapter(() -> LogDiff.of(emptyMap(), emptyList()));
		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		OTRemoteSql<LogDiff<CubeDiff>> otRemoteSql = OTRemoteSql.create(executor, dataSource, otSystem, diffAdapter);
		AsyncFunction<Integer, OTCommit<Integer, LogDiff<CubeDiff>>> loader = createLoader(otRemoteSql);

		StringBuilder sb = new StringBuilder();
		CompletableFuture<Void> future = otRemoteSql.getHeads()
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
		Predicate<Integer> visitOnceFilter = visitOnceFilter();
		return integer -> integer >= n && visitOnceFilter.test(integer);
	}

	private static <D> AsyncFunction<Integer, OTCommit<Integer, D>> createLoader(OTRemoteSql<D> otRemoteSql) {
		return commitId -> {
			System.out.println("load commit id: " + commitId);
			return GraphUtils.sqlLoaderWithoutCheckpointAndDiffs(otRemoteSql).apply(commitId);
		};
	}
}
