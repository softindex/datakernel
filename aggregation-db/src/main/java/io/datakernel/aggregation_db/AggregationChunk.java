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

import java.sql.Timestamp;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;

public class AggregationChunk {
	public static class NewChunk {
		private final long chunkId;
		private final String aggregationId;
		private final List<String> fields;
		private final PrimaryKey minPrimaryKey;
		private final PrimaryKey maxPrimaryKey;
		private final int count;

		public NewChunk(long chunkId, String aggregationId, List<String> fields, PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey, int count) {
			this.chunkId = chunkId;
			this.aggregationId = aggregationId;
			this.fields = fields;
			this.minPrimaryKey = minPrimaryKey;
			this.maxPrimaryKey = maxPrimaryKey;
			this.count = count;
		}
	}

	public static AggregationChunk createCommitChunk(int revisionId, NewChunk newChunk) {
		return new AggregationChunk(revisionId, newChunk.chunkId, newChunk.aggregationId, revisionId, revisionId,
				null, newChunk.fields, newChunk.minPrimaryKey, newChunk.maxPrimaryKey, newChunk.count);
	}

	public static AggregationChunk createConsolidateChunk(int revisionId, List<AggregationChunk> originalChunks, NewChunk newChunk) {
		checkArgument(!originalChunks.isEmpty());
		long[] sourceChunkIds = new long[originalChunks.size()];
		int minRevisionId = Integer.MAX_VALUE;
		int maxRevisionId = 0;
		for (int i = 0; i < originalChunks.size(); i++) {
			AggregationChunk aggregationChunk = originalChunks.get(i);
			sourceChunkIds[i] = aggregationChunk.chunkId;
			minRevisionId = Math.min(minRevisionId, aggregationChunk.revisionId);
			maxRevisionId = Math.max(maxRevisionId, aggregationChunk.revisionId);
		}
		return new AggregationChunk(revisionId, newChunk.chunkId, newChunk.aggregationId, minRevisionId, maxRevisionId, sourceChunkIds,
				newChunk.fields, newChunk.minPrimaryKey, newChunk.maxPrimaryKey, newChunk.count);
	}

	private final int revisionId;
	private final long chunkId;
	private final String aggregationId;
	private final int minRevisionId;
	private final int maxRevisionId;
	private final long[] sourceChunkIds;
	private final List<String> fields;
	private final PrimaryKey minPrimaryKey;
	private final PrimaryKey maxPrimaryKey;
	private final int count;
	private Integer consolidatedRevisionId;
	private Timestamp consolidationStarted;
	private Timestamp consolidationCompleted;

	public AggregationChunk(int revisionId, long chunkId,
	                        String aggregationId, int minRevisionId, int maxRevisionId,
	                        long[] sourceChunkIds,
	                        List<String> fields,
	                        PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey,
	                        int count) {
		this.revisionId = revisionId;
		this.chunkId = chunkId;
		this.aggregationId = aggregationId;
		this.minRevisionId = minRevisionId;
		this.maxRevisionId = maxRevisionId;
		this.sourceChunkIds = sourceChunkIds;
		this.fields = fields;
		this.minPrimaryKey = minPrimaryKey;
		this.maxPrimaryKey = maxPrimaryKey;
		this.count = count;
	}

	public long getChunkId() {
		return chunkId;
	}

	public String getAggregationId() {
		return aggregationId;
	}

	public int getRevisionId() {
		return revisionId;
	}

	public int getMinRevisionId() {
		return minRevisionId;
	}

	public int getMaxRevisionId() {
		return maxRevisionId;
	}

	public long[] getSourceChunkIds() {
		return sourceChunkIds;
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

	public boolean isConsolidated() {
		return consolidatedRevisionId != null;
	}

	public Integer getConsolidatedRevisionId() {
		return consolidatedRevisionId;
	}

	public void setConsolidatedRevisionId(Integer consolidatedRevisionId) {
		this.consolidatedRevisionId = consolidatedRevisionId;
	}

	public Timestamp getConsolidationStarted() {
		return consolidationStarted;
	}

	public void setConsolidationStarted(Timestamp consolidationStarted) {
		this.consolidationStarted = consolidationStarted;
	}

	public Timestamp getConsolidationCompleted() {
		return consolidationCompleted;
	}

	public void setConsolidationCompleted(Timestamp consolidationCompleted) {
		this.consolidationCompleted = consolidationCompleted;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("id", chunkId)
				.add("minPrimaryKey", minPrimaryKey)
				.add("maxPrimaryKey", maxPrimaryKey)
				.toString();
	}
}
