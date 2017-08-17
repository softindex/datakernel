package io.datakernel.logfs.ot;

import io.datakernel.stream.StreamProducer;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface LogDataConsumer<T, D> {
	CompletionStage<List<D>> consume(StreamProducer<T> logStream);
}
