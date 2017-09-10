package io.datakernel.logfs.ot;

import io.datakernel.stream.StreamConsumerWithResult;

import java.util.List;

public interface LogDataConsumer<T, D> {
	StreamConsumerWithResult<T, List<D>> consume();
}
