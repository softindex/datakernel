package io.datakernel.stream;

import java.util.concurrent.CompletionStage;

public interface StreamingCompletion {
	CompletionStage<Void> getProducerEndOfStream();

	CompletionStage<Void> getConsumerEndOfStream();

	CompletionStage<Void> getEndOfStream();
}
