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

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

final class AggregationQueryPlan {
	private AggregationQueryPlan() {}

	static AggregationQueryPlan create() {return new AggregationQueryPlan();}

	private static class FieldsWithChunks {
		private final List<String> fields;
		private final List<AggregationChunk> chunks;
		private final boolean sorted;

		public FieldsWithChunks(List<String> fields, List<AggregationChunk> chunks, boolean sorted) {
			this.fields = fields;
			this.chunks = chunks;
			this.sorted = sorted;
		}

		@Override
		public String toString() {
			return "{" +
					"fields=" + fields +
					", chunks=" + chunksToString(chunks) +
					", sorted=" + sorted +
					'}';
		}
	}

	private List<FieldsWithChunks> sequentialChunkGroups = newArrayList();
	private boolean postFiltering;
	private boolean optimizedAwayReducer;

	public void addChunkGroup(List<String> fields, List<AggregationChunk> sequentialChunkGroup, boolean sorted) {
		sequentialChunkGroups.add(new FieldsWithChunks(fields, sequentialChunkGroup, sorted));
	}

	public void setPostFiltering(boolean postFiltering) {
		this.postFiltering = postFiltering;
	}

	public void setOptimizedAwayReducer(boolean optimizedAwayReducer) {
		this.optimizedAwayReducer = optimizedAwayReducer;
	}

	public static String chunksToString(List<AggregationChunk> chunks) {
		Iterator<AggregationChunk> it = chunks.iterator();
		if (!it.hasNext())
			return "[]";

		StringBuilder sb = new StringBuilder();
		sb.append('[');
		for (; ; ) {
			AggregationChunk chunk = it.next();
			sb.append("{" + "revision=").append(chunk.getRevisionId())
					.append(", id=").append(chunk.getChunkId())
					.append(", minKey=").append(chunk.getMinPrimaryKey())
					.append(", maxKey=").append(chunk.getMaxPrimaryKey())
					.append(", count=").append(chunk.getCount())
					.append('}');
			if (!it.hasNext())
				return sb.append(']').toString();
			sb.append(',').append(' ');
		}
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

		sb.append("\nPost-filtering: ").append(postFiltering).append(". ");
		sb.append("\nOptimized away reducer: ").append(optimizedAwayReducer).append(". ");

		return sb.toString();
	}
}
