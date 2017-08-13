package io.datakernel.cube.ot;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.aggregation.Aggregation;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.aggregation.ot.AggregationDiffJson;
import io.datakernel.cube.Cube;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class CubeDiffJson extends TypeAdapter<CubeDiff> {
	private final Map<String, AggregationDiffJson> aggregationDiffJsons;

	private CubeDiffJson(Map<String, AggregationDiffJson> aggregationDiffJsons) {
		this.aggregationDiffJsons = aggregationDiffJsons;
	}

	public static CubeDiffJson create(Cube cube) {
		Map<String, AggregationDiffJson> aggregationDiffJsons = new LinkedHashMap<>();

		for (String aggregationId : cube.getAggregationIds()) {
			Aggregation aggregation = cube.getAggregation(aggregationId);
			AggregationDiffJson aggregationDiffJson = AggregationDiffJson.create(aggregation.getStructure());
			aggregationDiffJsons.put(aggregationId, aggregationDiffJson);
		}
		return new CubeDiffJson(aggregationDiffJsons);
	}

	@Override
	public void write(JsonWriter out, CubeDiff cubeDiff) throws IOException {
		out.beginObject();
		for (String aggregation : aggregationDiffJsons.keySet()) {
			AggregationDiff aggregationDiff = cubeDiff.get(aggregation);
			if (aggregationDiff == null)
				continue;
			AggregationDiffJson aggregationDiffJson = aggregationDiffJsons.get(aggregation);
			out.name(aggregation);
			aggregationDiffJson.write(out, aggregationDiff);
		}
		out.endObject();
	}

	@Override
	public CubeDiff read(JsonReader in) throws IOException {
		Map<String, AggregationDiff> map = new LinkedHashMap<>();
		in.beginObject();
		while (in.hasNext()) {
			String aggregation = in.nextName();
			AggregationDiffJson aggregationDiffJson = aggregationDiffJsons.get(aggregation);
			AggregationDiff aggregationDiff = aggregationDiffJson.read(in);
			map.put(aggregation, aggregationDiff);
		}
		in.endObject();
		return CubeDiff.of(map);
	}
}
