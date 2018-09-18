package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.Stage;

public interface SerialQueue<T> extends Cancellable {
	Stage<Void> put(@Nullable T value);

	Stage<T> take();

	default SerialConsumer<T> getConsumer() {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				return put(value);
			}
		};
	}

	default SerialConsumer<T> getConsumer(Stage<Void> acknowledge) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			public Stage<Void> accept(T value) {
				if (value != null) {
					return put(value);
				}
				put(null);
				return acknowledge;
			}
		};
	}

	default SerialSupplier<T> getSupplier() {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			public Stage<T> get() {
				return take();
			}
		};
	}

}
