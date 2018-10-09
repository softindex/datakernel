package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.MaterializedStage;
import io.datakernel.async.Stage;

public interface SerialQueue<T> extends Cancellable {
	Stage<Void> put(@Nullable T value);

	Stage<T> take();

	default SerialConsumer<T> getConsumer() {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			protected Stage<Void> doAccept(T value) {
				return put(value);
			}
		};
	}

	default SerialConsumer<T> getConsumer(MaterializedStage<Void> acknowledgement) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			protected Stage<Void> doAccept(T value) {
				if (value != null) return put(value);
				return put(null).both(acknowledgement);
			}
		};
	}

	default SerialSupplier<T> getSupplier() {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			protected Stage<T> doGet() {
				return take();
			}
		};
	}

}
