package io.datakernel.etl;

import io.datakernel.datastream.StreamConsumerWithResult;

import java.util.List;

public interface LogDataConsumer<T, D> {
	StreamConsumerWithResult<T, List<D>> consume();
}
