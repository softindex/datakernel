package io.datakernel.stream;

import java.util.concurrent.CompletionStage;

public interface HasStreamResult<X> {
	CompletionStage<X> getResult();
}
