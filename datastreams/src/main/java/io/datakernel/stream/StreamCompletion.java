package io.datakernel.stream;

import io.datakernel.async.Stage;

public interface StreamCompletion {
	Stage<Void> getProducerEndOfStream();

	Stage<Void> getConsumerEndOfStream();

	Stage<Void> getEndOfStream();
}
