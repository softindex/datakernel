package io.datakernel.stream;

import java.util.concurrent.CompletionStage;

public interface StreamingProducerResult<X> extends StreamingCompletion {
	CompletionStage<X> getProducerResult();
}
