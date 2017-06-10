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

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public final class QueryPlan {
	private final List<Sequence> sequences;

	public QueryPlan(List<Sequence> sequences) {
		this.sequences = sequences;
	}

	public static class Sequence {
		private final List<String> fields;
		private final List<AggregationChunk> chunks = new ArrayList<>();

		public List<String> getFields() {
			return unmodifiableList(fields);
		}

		public List<AggregationChunk> getChunks() {
			return unmodifiableList(chunks);
		}

		public void add(AggregationChunk chunk) {
			chunks.add(chunk);
		}

		public Sequence(List<String> fields) {
			this.fields = fields;
		}

		@Override
		public String toString() {
			return Lists.transform(chunks, new Function<AggregationChunk, String>() {
				@Override
				public String apply(AggregationChunk chunk) {
					return "" + chunk.getChunkId();
				}
			}).toString();
		}
	}

	public List<Sequence> getSequences() {
		return unmodifiableList(sequences);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("\nSequences (" + sequences.size() + "): ");

		for (int i = 0; i < sequences.size(); ++i) {
			sb.append("\n");
			sb.append((i + 1) + " (" + sequences.get(i).chunks.size() + "). ");
			sb.append(sequences.get(i) + " ");
		}

		return sb.toString();
	}

}
