package io.datakernel.async;

public interface AsyncProcess extends Cancellable {
	Stage<Void> getProcessResult();

	Stage<Void> startProcess();
}
