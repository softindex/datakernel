package io.datakernel.async.process;

import io.datakernel.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public interface AsyncProcess extends AsyncCloseable {
	@Contract(pure = true)
	@NotNull
	Promise<Void> getProcessCompletion();

	@NotNull
	Promise<Void> startProcess();
}
