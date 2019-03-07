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

import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.etl.LogDiff;
import io.datakernel.etl.LogOTProcessor;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRepositoryMySql;
import io.datakernel.ot.OTStateManager;

import java.io.IOException;
import java.sql.SQLException;

import static io.datakernel.async.TestUtils.await;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;

public final class TestUtils {

	public static void initializeRepository(OTRepositoryMySql<LogDiff<CubeDiff>> repository) {
		try {
			repository.initialize();
			repository.truncateTables();
		} catch (IOException | SQLException e) {
			throw new AssertionError(e);
		}
		Long id = await(repository.createCommitId());
		await(repository.pushAndUpdateHead(OTCommit.ofRoot(id)));
		await(repository.saveSnapshot(id, emptyList()));
	}

	public static <T> void runProcessLogs(AggregationChunkStorage<Long> aggregationChunkStorage, OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager, LogOTProcessor<T, CubeDiff> logOTProcessor) {
		LogDiff<CubeDiff> logDiff = await(logOTProcessor.processLog());
		await(aggregationChunkStorage
				.finish(logDiff.diffs().flatMap(CubeDiff::addedChunks).map(id -> (long) id).collect(toSet())));
		logCubeStateManager.add(logDiff);
		await(logCubeStateManager.sync());
	}

}
