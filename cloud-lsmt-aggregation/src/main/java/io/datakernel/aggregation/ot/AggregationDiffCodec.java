/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.aggregation.ot;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkCodec;
import io.datakernel.aggregation.PrimaryKey;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredInput;
import io.datakernel.codec.StructuredOutput;
import io.datakernel.common.exception.parse.ParseException;

import java.util.Collections;
import java.util.Set;

import static io.datakernel.aggregation.util.Utils.getPrimaryKeyCodec;
import static io.datakernel.codec.StructuredCodecs.ofSet;
import static io.datakernel.codec.json.JsonUtils.oneline;

public class AggregationDiffCodec implements StructuredCodec<AggregationDiff> {
	public static final String ADDED = "added";
	public static final String REMOVED = "removed";

	private final StructuredCodec<Set<AggregationChunk>> aggregationChunksCodec;

	private AggregationDiffCodec(AggregationChunkCodec aggregationChunksCodec) {
		this.aggregationChunksCodec = ofSet(oneline(aggregationChunksCodec));
	}

	public static AggregationDiffCodec create(AggregationStructure structure) {
		Set<String> allowedMeasures = structure.getMeasureTypes().keySet();
		StructuredCodec<PrimaryKey> primaryKeyCodec = getPrimaryKeyCodec(structure);
		return new AggregationDiffCodec(AggregationChunkCodec.create(structure.getChunkIdCodec(), primaryKeyCodec, allowedMeasures));
	}

	@Override
	public void encode(StructuredOutput out, AggregationDiff diff) {
		out.writeObject(() -> {
			out.writeKey(ADDED);
			aggregationChunksCodec.encode(out, diff.getAddedChunks());
			if (!diff.getRemovedChunks().isEmpty()) {
				out.writeKey(REMOVED);
				aggregationChunksCodec.encode(out, diff.getRemovedChunks());
			}
		});
	}

	@Override
	public AggregationDiff decode(StructuredInput in) throws ParseException {
		return in.readObject($ -> {
			in.readKey(ADDED);
			Set<AggregationChunk> added = aggregationChunksCodec.decode(in);
			Set<AggregationChunk> removed = Collections.emptySet();
			if (in.hasNext()) {
				in.readKey(REMOVED);
				removed = aggregationChunksCodec.decode(in);
			}
			return AggregationDiff.of(added, removed);
		});
	}

}
