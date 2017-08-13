package io.datakernel.logfs.ot;

import io.datakernel.async.ResultCallback;
import io.datakernel.stream.StreamProducer;

import java.util.List;

public interface LogDataConsumer<T, D> {
	void consume(StreamProducer<T> logStream, ResultCallback<List<D>> callback);
}
