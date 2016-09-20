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

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.aggregation_db.AggregationQueryPlan.chunksToString;

public final class ConsolidationPlan {
	private ConsolidationPlan() {}

	public static ConsolidationPlan create() {return new ConsolidationPlan();}

	private static class FieldsWithChunks {
		private final List<String> fields;
		private final List<AggregationChunk> chunks;

		public FieldsWithChunks(List<String> fields, List<AggregationChunk> chunks) {
			this.fields = fields;
			this.chunks = chunks;
		}

		@Override
		public String toString() {
			return "{" +
					"fields=" + fields +
					", chunks=" + chunksToString(chunks) +
					'}';
		}
	}

	private List<FieldsWithChunks> sequentialChunkGroups = newArrayList();

	public void addChunkGroup(List<String> fields, List<AggregationChunk> sequentialChunkGroup) {
		sequentialChunkGroups.add(new FieldsWithChunks(fields, sequentialChunkGroup));
	}

	@Override
	public String toString() {
		if (sequentialChunkGroups.isEmpty())
			return "empty";

		StringBuilder sb = new StringBuilder();

		sb.append("\nSequential groups (").append(sequentialChunkGroups.size()).append("): ");

		for (int i = 0; i < sequentialChunkGroups.size(); ++i) {
			sb.append("\n");
			sb.append(i + 1).append(" (").append(sequentialChunkGroups.get(i).chunks.size()).append("). ");
			sb.append(sequentialChunkGroups.get(i)).append(" ");
		}

		return sb.toString();
	}
}
