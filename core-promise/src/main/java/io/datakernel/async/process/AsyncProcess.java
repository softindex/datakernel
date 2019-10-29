package io.datakernel.async.process;

import io.datakernel.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public interface AsyncProcess extends Cancellable {
	@Contract(pure = true)
	@NotNull
	Promise<Void> getProcessCompletion();

	@NotNull
	Promise<Void> startProcess();
}
