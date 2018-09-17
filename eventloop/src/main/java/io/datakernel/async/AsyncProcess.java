package io.datakernel.async;

public interface AsyncProcess extends Cancellable {
	Stage<Void> getResult();

	Stage<Void> start();
}
