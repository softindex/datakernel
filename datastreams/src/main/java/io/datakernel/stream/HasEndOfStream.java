package io.datakernel.stream;

import java.util.concurrent.CompletionStage;

public interface HasEndOfStream {
	CompletionStage<Void> getEndOfStream();
}
