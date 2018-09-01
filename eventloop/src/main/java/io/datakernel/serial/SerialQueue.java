package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.Stage;

public interface SerialQueue<T> extends Cancellable {
	Stage<Void> put(@Nullable T value);

	Stage<T> take();
}
