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
	private static class FieldsWithChunks {
		private List<String> fields;
		private List<AggregationChunk> chunks;

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

		private static String chunksToString(List<AggregationChunk> chunks) {
			Iterator<AggregationChunk> it = chunks.iterator();
			if (!it.hasNext())
				return "[]";

			StringBuilder sb = new StringBuilder();
			sb.append('[');
			for (;;) {
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
	}

	private List<FieldsWithChunks> sequentialChunkGroups = newArrayList();
	private boolean postFiltering;
	private boolean additionalSorting;

	public void addChunkGroup(List<String> fields, List<AggregationChunk> sequentialChunkGroup) {
		sequentialChunkGroups.add(new FieldsWithChunks(fields, sequentialChunkGroup));
	}

	public void setPostFiltering(boolean postFiltering) {
		this.postFiltering = postFiltering;
	}

	public void setAdditionalSorting(boolean additionalSorting) {
		this.additionalSorting = additionalSorting;
	}

	@Override
	public String toString() {
		if (sequentialChunkGroups.isEmpty())
			return "empty";

		StringBuilder sb = new StringBuilder();

		sb.append("\nSequential groups: ");

		for (int i = 0; i < sequentialChunkGroups.size(); ++i) {
			sb.append("\n");
			sb.append(i + 1);
			sb.append(". ");
			sb.append(sequentialChunkGroups.get(i));
			sb.append(" ");
		}

		sb.append("\nPost-filtering: ");
		sb.append(postFiltering);
		sb.append(". ");

		sb.append("\nAdditional sorting: ");
		sb.append(additionalSorting);
		sb.append(". ");

		return sb.toString();
	}
}
