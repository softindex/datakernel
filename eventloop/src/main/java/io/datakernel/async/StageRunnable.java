package io.datakernel.async;

import java.util.concurrent.CompletionStage;

public interface StageRunnable {
	CompletionStage<Void> run();
}
