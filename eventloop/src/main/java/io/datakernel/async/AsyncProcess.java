package io.datakernel.async;

public interface AsyncProcess extends Cancellable {
	Stage<Void> process();
}
