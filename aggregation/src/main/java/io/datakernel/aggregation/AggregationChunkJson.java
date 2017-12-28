package io.datakernel.aggregation;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.utils.GsonAdapters.STRING_JSON;
import static io.datakernel.utils.GsonAdapters.ofList;

public class AggregationChunkJson extends TypeAdapter<AggregationChunk> {
	public static final String ID = "id";
	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String COUNT = "count";
	public static final String MEASURES = "measures";

	private final TypeAdapter<PrimaryKey> primaryKeyTypeAdapter;
	private final TypeAdapter<List<String>> stringListAdapter;
	private final Set<String> allowedMeasures;

	private AggregationChunkJson(TypeAdapter<PrimaryKey> primaryKeyTypeAdapter, TypeAdapter<List<String>> stringListAdapter, Set<String> allowedMeasures) {
		this.primaryKeyTypeAdapter = primaryKeyTypeAdapter;
		this.stringListAdapter = stringListAdapter;
		this.allowedMeasures = allowedMeasures;
	}

	public static AggregationChunkJson create(TypeAdapter<PrimaryKey> primaryKeyTypeAdapter, Set<String> allowedMeasures) {
		return new AggregationChunkJson(primaryKeyTypeAdapter, ofList(STRING_JSON), allowedMeasures);
	}

	@Override
	public void write(JsonWriter writer, AggregationChunk chunk) throws IOException {
		writer.beginObject();

		writer.name(ID);
		writer.value(chunk.getChunkId());

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

		checkArgument(ID.equals(reader.nextName()));
		int id = reader.nextInt();

		checkArgument(MIN.equals(reader.nextName()));
		PrimaryKey from = primaryKeyTypeAdapter.read(reader);

		checkArgument(MAX.equals(reader.nextName()));
		PrimaryKey to = primaryKeyTypeAdapter.read(reader);

		checkArgument(COUNT.equals(reader.nextName()));
		int count = reader.nextInt();

		checkArgument(MEASURES.equals(reader.nextName()));
		List<String> measures = stringListAdapter.read(reader);

		final List<String> invalidMeasures = getInvalidMeasures(measures);
		if (!invalidMeasures.isEmpty()) throw new IOException("Unknown fields: " + invalidMeasures);

		reader.endObject();

		return AggregationChunk.create(id, measures, from, to, count);
	}

	private List<String> getInvalidMeasures(List<String> measures) {
		final List<String> invalidMeasures = new ArrayList<>();
		for (String measure : measures) {
			if (!allowedMeasures.contains(measure)) {
				invalidMeasures.add(measure);
			}
		}
		return invalidMeasures;
	}

}
