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

import com.google.common.base.MoreObjects;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;

public class AggregationChunk {
	public static class NewChunk {
		public final long chunkId;
		public final List<String> fields;
		public final PrimaryKey minPrimaryKey;
		public final PrimaryKey maxPrimaryKey;
		public final int count;

		public NewChunk(long chunkId, List<String> fields, PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey, int count) {
			this.chunkId = chunkId;
			this.fields = fields;
			this.minPrimaryKey = minPrimaryKey;
			this.maxPrimaryKey = maxPrimaryKey;
			this.count = count;
		}

		@Override
		public String toString() {
			return MoreObjects.toStringHelper(this)
					.add("chunkId", chunkId)
					.add("fields", fields)
					.add("minPrimaryKey", minPrimaryKey)
					.add("maxPrimaryKey", maxPrimaryKey)
					.add("count", count)
					.toString();
		}
	}

	public static AggregationChunk createChunk(int revisionId, NewChunk newChunk) {
		return new AggregationChunk(revisionId, newChunk.chunkId,
				newChunk.fields, newChunk.minPrimaryKey, newChunk.maxPrimaryKey, newChunk.count);
	}

	private final int revisionId;
	private final long chunkId;
	private final List<String> fields;
	private final PrimaryKey minPrimaryKey;
	private final PrimaryKey maxPrimaryKey;
	private final int count;

	public AggregationChunk(int revisionId, long chunkId,
	                        List<String> fields,
	                        PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey,
	                        int count) {
		this.revisionId = revisionId;
		this.chunkId = chunkId;
		this.fields = fields;
		this.minPrimaryKey = minPrimaryKey;
		this.maxPrimaryKey = maxPrimaryKey;
		this.count = count;
	}

	public long getChunkId() {
		return chunkId;
	}

	public int getRevisionId() {
		return revisionId;
	}

	public List<String> getFields() {
		return unmodifiableList(fields);
	}

	public PrimaryKey getMinPrimaryKey() {
		return minPrimaryKey;
	}

	public PrimaryKey getMaxPrimaryKey() {
		return maxPrimaryKey;
	}

	public int getCount() {
		return count;
	}

	@Override
	public String toString() {
		return "{" +
				"revision=" + revisionId +
				", id=" + chunkId +
				", fields=" + fields +
				", minKey=" + minPrimaryKey +
				", maxKey=" + maxPrimaryKey +
				", count=" + count +
				'}';
	}
}
