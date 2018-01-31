package io.datakernel.stream;

import io.datakernel.async.Stage;

public interface StreamProducerResult<X> extends StreamCompletion {
	Stage<X> getProducerResult();
}
