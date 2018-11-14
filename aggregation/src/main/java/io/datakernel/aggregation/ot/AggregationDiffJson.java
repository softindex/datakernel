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

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkJson;
import io.datakernel.aggregation.PrimaryKey;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static io.datakernel.aggregation.AggregationUtils.getPrimaryKeyJson;
import static io.datakernel.json.GsonAdapters.ofSet;
import static io.datakernel.json.GsonAdapters.oneline;
import static io.datakernel.util.Preconditions.checkArgument;

public class AggregationDiffJson extends TypeAdapter<AggregationDiff> {
	public static final String ADDED = "added";
	public static final String REMOVED = "removed";

	private final TypeAdapter<Set<AggregationChunk>> aggregationChunksJson;

	private AggregationDiffJson(AggregationChunkJson aggregationChunksJson) {
		this.aggregationChunksJson = ofSet(oneline(aggregationChunksJson));
	}

	public static AggregationDiffJson create(AggregationStructure structure) {
		Set<String> allowedMeasures = structure.getMeasureTypes().keySet();
		TypeAdapter<PrimaryKey> primaryKeyJson = getPrimaryKeyJson(structure);
		return new AggregationDiffJson(AggregationChunkJson.create(structure.getChunkIdScheme(), primaryKeyJson, allowedMeasures));
	}

	@Override
	public void write(JsonWriter writer, AggregationDiff diff) throws IOException {
		writer.beginObject();

		writer.name(ADDED);
		aggregationChunksJson.write(writer, diff.getAddedChunks());

		if (!diff.getRemovedChunks().isEmpty()) {
			writer.name(REMOVED);
			aggregationChunksJson.write(writer, diff.getRemovedChunks());
		}

		writer.endObject();
	}

	@Override
	public AggregationDiff read(JsonReader reader) throws IOException {
		reader.beginObject();

		checkArgument(ADDED.equals(reader.nextName()), "Malformed json object, should have name 'added'");
		Set<AggregationChunk> added = aggregationChunksJson.read(reader);

		Set<AggregationChunk> removed = Collections.emptySet();
		if (reader.hasNext()) {
			checkArgument(REMOVED.equals(reader.nextName()), "Malformed json object, should have name 'removed'");
			removed = aggregationChunksJson.read(reader);
		}

		reader.endObject();

		return AggregationDiff.of(added, removed);
	}

}
