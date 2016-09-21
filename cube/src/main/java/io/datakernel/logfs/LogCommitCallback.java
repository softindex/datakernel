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

package io.datakernel.logfs;

import com.google.common.collect.Multimap;
import io.datakernel.aggregation_db.AggregationChunk;
import io.datakernel.aggregation_db.AggregationMetadata;
import io.datakernel.async.CallbackRegistry;
import io.datakernel.async.ExceptionCallback;

import java.util.Map;

public abstract class LogCommitCallback extends ExceptionCallback {
	public final void onCommit(String log,
	                           Map<String, LogPosition> oldPositions,
	                           Map<String, LogPosition> newPositions,
	                           Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks) {
		CallbackRegistry.complete(this);
		handleCommit(log, oldPositions, newPositions, newChunks);
	}

	protected abstract void handleCommit(String log, Map<String, LogPosition> oldPositions,
	                                     Map<String, LogPosition> newPositions,
	                                     Multimap<AggregationMetadata, AggregationChunk.NewChunk> newChunks);
}
