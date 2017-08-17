package io.datakernel.aggregation;

import java.util.concurrent.CompletionStage;

public interface IdGenerator<K> {
	CompletionStage<K> createId();
}
