/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.cube;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.aggregation_db.Aggregation;
import io.datakernel.aggregation_db.AggregationMetadata;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.aggregation_db.sql.tables.records.AggregationDbStructureRecord;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.impl.DSL;

import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.aggregation_db.sql.tables.AggregationDbStructure.AGGREGATION_DB_STRUCTURE;
import static io.datakernel.async.AsyncCallbacks.runConcurrently;

/**
 * Stores cube metadata in relational database.
 */
public final class CubeMetadataStorageSql implements CubeMetadataStorage {
	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Configuration jooqConfiguration;

	/**
	 * Constructs a cube metadata storage, that runs in the specified event loop, performs SQL queries in the given executor,
	 * and connects to RDBMS using the specified configuration.
	 *
	 * @param eventloop         event loop, in which metadata storage is to run
	 * @param executor          executor, where SQL queries are to be run
	 * @param jooqConfiguration database connection configuration
	 */
	public CubeMetadataStorageSql(Eventloop eventloop, ExecutorService executor, Configuration jooqConfiguration) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.jooqConfiguration = jooqConfiguration;
	}

	private void loadAggregations(DSLContext jooq, Cube cube) {
		Result<AggregationDbStructureRecord> records = jooq
				.selectFrom(AGGREGATION_DB_STRUCTURE)
				.where(AGGREGATION_DB_STRUCTURE.ID.notIn(cube.getAggregations().keySet()))
				.fetch();

		Splitter splitter = Splitter.on(' ').omitEmptyStrings();
		Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.QueryPredicates.class, new QueryPredicatesGsonSerializer(cube.getStructure()))
				.create();

		for (AggregationDbStructureRecord record : records) {
			AggregationMetadata aggregationMetadata = new AggregationMetadata(record.getId(),
					newArrayList(splitter.split(record.getKeys())),
					newArrayList(splitter.split(record.getInputfields())),
					newArrayList(splitter.split(record.getOutputfields())),
					gson.fromJson(record.getPredicates(), AggregationQuery.QueryPredicates.class));
			cube.addAggregation(aggregationMetadata);
		}
	}

	// TODO (dtkachenko): return loaded aggregations metadatas
	@Override
	public void loadAggregations(final Cube cube, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				loadAggregations(DSL.using(jooqConfiguration), cube);
			}
		}, callback);
	}

	private void saveAggregations(DSLContext jooq, Cube cube) {
		Joiner joiner = Joiner.on(' ');
		Gson gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.QueryPredicates.class, new QueryPredicatesGsonSerializer(cube.getStructure()))
				.create();
		for (Aggregation aggregation : cube.getAggregations().values()) {
			jooq.insertInto(AGGREGATION_DB_STRUCTURE)
					.set(new AggregationDbStructureRecord(
							aggregation.getId(),
							joiner.join(aggregation.getKeys()),
							joiner.join(aggregation.getInputFields()),
							joiner.join(aggregation.getOutputFields()),
							gson.toJson(aggregation.getAggregationPredicates())))
					.onDuplicateKeyIgnore()
					.execute();
		}
	}

	// TODO (dtkachenko): save list of exlicitely provided list of aggregations metadatas
	@Override
	public void saveAggregations(final Cube cube, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new Runnable() {
			@Override
			public void run() {
				saveAggregations(DSL.using(jooqConfiguration), cube);
			}
		}, callback);
	}
}
