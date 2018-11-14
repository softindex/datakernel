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

package io.datakernel.aggregation;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.datakernel.json.GsonAdapters.STRING_JSON;
import static io.datakernel.json.GsonAdapters.ofList;
import static io.datakernel.util.Preconditions.checkArgument;

public class AggregationChunkJson extends TypeAdapter<AggregationChunk> {
	public static final String ID = "id";
	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String COUNT = "count";
	public static final String MEASURES = "measures";

	private final ChunkIdScheme<Object> chunkIdScheme;
	private final TypeAdapter<PrimaryKey> primaryKeyTypeAdapter;
	private final TypeAdapter<List<String>> stringListAdapter;
	private final Set<String> allowedMeasures;

	@SuppressWarnings("unchecked")
	private AggregationChunkJson(ChunkIdScheme<?> chunkIdScheme,
			TypeAdapter<PrimaryKey> primaryKeyTypeAdapter,
			TypeAdapter<List<String>> stringListAdapter,
			Set<String> allowedMeasures) {
		this.chunkIdScheme = (ChunkIdScheme<Object>) chunkIdScheme;
		this.primaryKeyTypeAdapter = primaryKeyTypeAdapter;
		this.stringListAdapter = stringListAdapter;
		this.allowedMeasures = allowedMeasures;
	}

	public static AggregationChunkJson create(ChunkIdScheme<?> chunkIdScheme,
			TypeAdapter<PrimaryKey> primaryKeyTypeAdapter,
			Set<String> allowedMeasures) {
		return new AggregationChunkJson(chunkIdScheme, primaryKeyTypeAdapter, ofList(STRING_JSON), allowedMeasures);
	}

	@Override
	public void write(JsonWriter writer, AggregationChunk chunk) throws IOException {
		writer.beginObject();

		writer.name(ID);
		chunkIdScheme.toJson(writer, chunk.getChunkId());

		writer.name(MIN);
		primaryKeyTypeAdapter.write(writer, chunk.getMinPrimaryKey());

		writer.name(MAX);
		primaryKeyTypeAdapter.write(writer, chunk.getMaxPrimaryKey());

		writer.name(COUNT);
		writer.value(chunk.getCount());

		writer.name(MEASURES);
		stringListAdapter.write(writer, chunk.getMeasures());

		writer.endObject();
	}

	@Override
	public AggregationChunk read(JsonReader reader) throws IOException {
		reader.beginObject();

		checkArgument(ID.equals(reader.nextName()), "Malformed json object, should have name 'id'");
		Object id = chunkIdScheme.fromJson(reader);

		checkArgument(MIN.equals(reader.nextName()), "Malformed json object, should have name 'min'");
		PrimaryKey from = primaryKeyTypeAdapter.read(reader);

		checkArgument(MAX.equals(reader.nextName()), "Malformed json object, should have name 'max'");
		PrimaryKey to = primaryKeyTypeAdapter.read(reader);

		checkArgument(COUNT.equals(reader.nextName()), "Malformed json object, should have name 'count'");
		int count = reader.nextInt();

		checkArgument(MEASURES.equals(reader.nextName()), "Malformed json object, should have name 'measures'");
		List<String> measures = stringListAdapter.read(reader);

		List<String> invalidMeasures = getInvalidMeasures(measures);
		if (!invalidMeasures.isEmpty()) throw new IOException("Unknown fields: " + invalidMeasures);

		reader.endObject();

		return AggregationChunk.create(id, measures, from, to, count);
	}

	private List<String> getInvalidMeasures(List<String> measures) {
		List<String> invalidMeasures = new ArrayList<>();
		for (String measure : measures) {
			if (!allowedMeasures.contains(measure)) {
				invalidMeasures.add(measure);
			}
		}
		return invalidMeasures;
	}

}
