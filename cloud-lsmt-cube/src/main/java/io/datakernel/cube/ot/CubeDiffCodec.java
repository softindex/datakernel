package io.datakernel.cube.ot;

import io.datakernel.aggregation.Aggregation;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.aggregation.ot.AggregationDiffCodec;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredInput;
import io.datakernel.codec.StructuredOutput;
import io.datakernel.common.parse.ParseException;
import io.datakernel.cube.Cube;

import java.util.LinkedHashMap;
import java.util.Map;

public class CubeDiffCodec implements StructuredCodec<CubeDiff> {
	private final Map<String, AggregationDiffCodec> aggregationDiffCodecs;

	private CubeDiffCodec(Map<String, AggregationDiffCodec> aggregationDiffCodecs) {
		this.aggregationDiffCodecs = aggregationDiffCodecs;
	}

	public static CubeDiffCodec create(Cube cube) {
		Map<String, AggregationDiffCodec> aggregationDiffCodecs = new LinkedHashMap<>();

		for (String aggregationId : cube.getAggregationIds()) {
			Aggregation aggregation = cube.getAggregation(aggregationId);
			AggregationDiffCodec aggregationDiffCodec = AggregationDiffCodec.create(aggregation.getStructure());
			aggregationDiffCodecs.put(aggregationId, aggregationDiffCodec);
		}
		return new CubeDiffCodec(aggregationDiffCodecs);
	}

	@Override
	public void encode(StructuredOutput out, CubeDiff cubeDiff) {
		out.writeObject(() -> {
			for (String aggregation : aggregationDiffCodecs.keySet()) {
				AggregationDiff aggregationDiff = cubeDiff.get(aggregation);
				if (aggregationDiff == null)
					continue;
				AggregationDiffCodec aggregationDiffCodec = aggregationDiffCodecs.get(aggregation);
				out.writeKey(aggregation);
				aggregationDiffCodec.encode(out, aggregationDiff);
			}
		});
	}

	@Override
	public CubeDiff decode(StructuredInput in) throws ParseException {
		return in.readObject($ -> {
			Map<String, AggregationDiff> map = new LinkedHashMap<>();
			while (in.hasNext()) {
				String aggregation = in.readKey();
				AggregationDiffCodec aggregationDiffCodec = aggregationDiffCodecs.get(aggregation);
				if (aggregationDiffCodec == null) {
					throw new ParseException("Unknown aggregation: " + aggregation);
				}
				AggregationDiff aggregationDiff = aggregationDiffCodec.decode(in);
				map.put(aggregation, aggregationDiff);
			}

			return CubeDiff.of(map);
		});
	}

}
