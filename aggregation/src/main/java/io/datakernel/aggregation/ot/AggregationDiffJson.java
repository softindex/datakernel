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
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.gson.GsonAdapters.ofSet;
import static io.datakernel.util.gson.GsonAdapters.oneline;

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

		checkArgument(ADDED.equals(reader.nextName()));
		Set<AggregationChunk> added = aggregationChunksJson.read(reader);

		Set<AggregationChunk> removed = Collections.emptySet();
		if (reader.hasNext()) {
			checkArgument(REMOVED.equals(reader.nextName()));
			removed = aggregationChunksJson.read(reader);
		}

		reader.endObject();

		return AggregationDiff.of(added, removed);
	}

}
