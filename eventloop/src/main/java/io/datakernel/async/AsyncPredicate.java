package io.datakernel.async;

import java.util.concurrent.CompletionStage;

public interface AsyncPredicate<T> {

	CompletionStage<Boolean> apply(T t);
}
