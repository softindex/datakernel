package io.datakernel.boot.cube;

import com.google.inject.*;
import com.zaxxer.hikari.HikariDataSource;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.IdGenerator;
import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.aggregation.fieldtype.FieldTypes;
import io.datakernel.aggregation.util.IdGeneratorSql;
import io.datakernel.aggregation.util.SqlAtomicSequence;
import io.datakernel.async.AsyncCallable;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.cube.Cube;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.cube.ot.CubeDiffJson;
import io.datakernel.cube.ot.CubeOT;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.logfs.ot.LogDiffJson;
import io.datakernel.logfs.ot.LogOT;
import io.datakernel.logfs.ot.LogOTState;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTRemoteSql;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.datakernel.util.MemSize;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.config.ConfigConverters.ofMemSize;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.cube.Cube.AggregationConfig.id;

public class ExampleCubeModule extends PrivateModule {
	@Override
	protected void configure() {
		bind(DataSource.class).to(HikariDataSource.class);
		expose(Cube.class);
		expose(Key.get(new TypeLiteral<OTStateManager<Integer, LogDiff<CubeDiff>>>() {}));
	}

	@Provides
	@Singleton
	Cube cube(Eventloop eventloop, ExecutorService executor, DefiningClassLoader classLoader, AggregationChunkStorage aggregationChunkStorage) {
		return Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("date", FieldTypes.ofLocalDate())
				.withDimension("advertiser", FieldTypes.ofInt())
				.withDimension("campaign", FieldTypes.ofInt())
				.withDimension("banner", FieldTypes.ofInt())
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign")
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withMeasure("conversions", sum(ofLong()))
				.withMeasure("revenue", sum(ofDouble()))
				.withAggregation(id("detailed")
						.withDimensions("date", "advertiser", "campaign", "banner")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withAggregation(id("date")
						.withDimensions("date")
						.withMeasures("impressions", "clicks", "conversions", "revenue"))
				.withAggregation(id("advertiser")
						.withDimensions("advertiser")
						.withMeasures("impressions", "clicks", "conversions", "revenue"));
	}

	@Provides
	@Singleton
	OTStateManager<Integer, LogDiff<CubeDiff>> otStateManager(Config config, Eventloop eventloop,
	                                                          OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms,
	                                                          LogOTState<CubeDiff> cubeDiffLogOTState) {
		return OTStateManager.create(eventloop, algorithms, cubeDiffLogOTState);
	}

	@Provides
	@Singleton
	OTAlgorithms<Integer, LogDiff<CubeDiff>> algorithms(Config config, Eventloop eventloop,
	                                                    OTSystem<LogDiff<CubeDiff>> otSystem,
	                                                    OTRemoteSql<LogDiff<CubeDiff>> otSourceSql) {
		return OTAlgorithms.create(eventloop, otSystem, otSourceSql, Integer::compare);
	}

	@Provides
	@Singleton
	LogOTState<CubeDiff> cubeDiffLogOTState(Cube cube) {
		return LogOTState.create(cube);
	}

	@Provides
	@Singleton
	OTRemoteSql<LogDiff<CubeDiff>> otSourceSql(Config config, Eventloop eventloop,
	                                           ExecutorService executor,
	                                           DataSource dataSource,
	                                           OTSystem<LogDiff<CubeDiff>> otSystem,
	                                           Cube cube) {
		return OTRemoteSql.create(eventloop, executor, dataSource, otSystem, LogDiffJson.create(CubeDiffJson.create(cube)));
	}

	@Provides
	@Singleton
	OTSystem<LogDiff<CubeDiff>> otSystem() {
		return LogOT.createLogOT(CubeOT.createCubeOT());
	}

	@Provides
	@Singleton
	AggregationChunkStorage aggregationChunkStorage(Config config, Eventloop eventloop,
	                                                ExecutorService executor,
	                                                IdGenerator<Long> idGenerator) {
		Path aggregationPath = config.get(ofPath(), "Aggregations.path");
		MemSize bufferSize = config.get(ofMemSize(), "Aggregations.bufferSize", MemSize.kilobytes(256));
		Path backupPath = config.get(ofPath(), "Aggregations.backupPath", aggregationPath.resolve(LocalFsChunkStorage.DEFAULT_BACKUP_FOLDER_NAME));
		return LocalFsChunkStorage.create(eventloop, executor, idGenerator, aggregationPath)
				.withBufferSize(bufferSize)
				.withBackupPath(backupPath);
	}

	@Provides
	@Singleton
	IdGenerator<Long> idGenerator(IdGeneratorSql idGeneratorSql) {
		AsyncCallable<Long> prefetch = AsyncCallable.of(idGeneratorSql::createId).prefetch(1);
		return prefetch::call;
	}

	@Provides
	@Singleton
	IdGeneratorSql idGenerator(Eventloop eventloop, ExecutorService executor, DataSource dataSource) {
		return IdGeneratorSql.create(eventloop, executor, dataSource,
				SqlAtomicSequence.ofLastInsertID("ot_chunks", "next"))
				.withStride(1000);
	}

	@Provides
	@Singleton
	HikariDataSource hikariDataSource(Config config) {
		return new HikariDataSource(config.get(ConfigConverters.ofHikariConfig(), "dataSource"));
	}

	@Provides
	@Singleton
	DefiningClassLoader definingClassLoader() {
		return DefiningClassLoader.create();
	}

	@Provides
	@Singleton
	ExecutorService executorService() {
		return Executors.newCachedThreadPool();
	}

}
