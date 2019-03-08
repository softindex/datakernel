package io.datakernel.async;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public interface AsyncProcess extends Cancellable {
	@Contract(pure = true)
	@NotNull
	MaterializedPromise<Void> getProcessCompletion();

	@NotNull
	MaterializedPromise<Void> startProcess();
}
