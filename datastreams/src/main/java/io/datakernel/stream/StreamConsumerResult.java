package io.datakernel.stream;

import io.datakernel.async.Stage;

public interface StreamConsumerResult<Y> extends StreamCompletion {
	Stage<Y> getConsumerResult();
}
