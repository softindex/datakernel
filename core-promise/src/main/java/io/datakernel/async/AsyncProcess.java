package io.datakernel.async;

public interface AsyncProcess extends Cancellable {
	Promise<Void> getProcessResult();

	Promise<Void> startProcess();
}
