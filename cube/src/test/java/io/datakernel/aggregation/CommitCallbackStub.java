package io.datakernel.aggregation;

import com.google.common.collect.Multimap;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.Cube;

import java.util.Map;

public class CommitCallbackStub extends ResultCallback<Multimap<String, AggregationChunk>> {
	private final Cube cube;
	private final CompletionCallback callback;

	public CommitCallbackStub(Cube cube) {
		this(cube, null);
	}

	public CommitCallbackStub(Cube cube, CompletionCallback callback) {
		this.cube = cube;
		this.callback = callback;
	}

	@Override
	public void onResult(Multimap<String, AggregationChunk> newChunks) {
		cube.incrementLastRevisionId();
		for (Map.Entry<String, AggregationChunk> entry : newChunks.entries()) {
			String aggregationId = entry.getKey();
			AggregationChunk newChunk = entry.getValue();
			cube.getAggregation(aggregationId).getMetadata().addToIndex(newChunk);
		}

		if (callback != null)
			callback.setComplete();
	}

	@Override
	public void onException(Exception exception) {
		if (callback != null)
			callback.setException(exception);
	}
}
