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

package io.datakernel.aggregation;

import com.google.common.collect.Multimap;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.Cube;

import java.util.Map;

import static io.datakernel.aggregation.AggregationChunk.createChunk;

public class CommitCallbackStub extends ResultCallback<Multimap<String, AggregationChunk.NewChunk>> {
	private final Cube cube;
	private final CompletionCallback callback;

	public CommitCallbackStub(Cube cube) {
		this(cube, null);
	}

	public CommitCallbackStub(Cube cube, CompletionCallback callback) {
		this.cube = cube;
		this.callback = callback;
	}

	@Override
	public void onResult(Multimap<String, AggregationChunk.NewChunk> newChunks) {
		cube.incrementLastRevisionId();
		for (Map.Entry<String, AggregationChunk.NewChunk> entry : newChunks.entries()) {
			String aggregationId = entry.getKey();
			AggregationChunk.NewChunk newChunk = entry.getValue();
			cube.getAggregation(aggregationId).getMetadata().addToIndex(createChunk(cube.getLastRevisionId(), newChunk));
		}

		if (callback != null)
			callback.setComplete();
	}

	@Override
	public void onException(Exception exception) {
		if (callback != null)
			callback.setException(exception);
	}
}
