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

package io.datakernel.aggregation_db;

import io.datakernel.async.ResultCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.datakernel.aggregation_db.AggregationChunk.createChunk;

public class AggregationCommitCallback implements ResultCallback<List<AggregationChunk.NewChunk>> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Aggregation aggregation;

	private AggregationCommitCallback(Aggregation aggregation) {
		this.aggregation = aggregation;
	}

	public static AggregationCommitCallback create(Aggregation aggregation) {
		return new AggregationCommitCallback(aggregation);
	}

	@Override
	public void onResult(List<AggregationChunk.NewChunk> result) {
		aggregation.incrementLastRevisionId();
		for (AggregationChunk.NewChunk newChunk : result) {
			aggregation.addToIndex(createChunk(aggregation.getLastRevisionId(), newChunk));
		}
	}

	@Override
	public void onException(Exception exception) {
		logger.error("Exception thrown while trying to commit to aggregation {}.", aggregation);
	}
}
