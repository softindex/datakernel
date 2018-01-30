package io.datakernel.stream;

import java.util.concurrent.CompletionStage;

public interface StreamingConsumerResult<Y> extends StreamingCompletion {
	CompletionStage<Y> getConsumerResult();
}
