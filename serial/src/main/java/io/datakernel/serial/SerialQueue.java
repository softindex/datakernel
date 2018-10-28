package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;

public interface SerialQueue<T> extends Cancellable {
	Promise<Void> put(@Nullable T value);

	Promise<T> take();

	default SerialConsumer<T> getConsumer() {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				return put(value);
			}
		};
	}

	default SerialConsumer<T> getConsumer(MaterializedPromise<Void> acknowledgement) {
		return new AbstractSerialConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				if (value != null) return put(value);
				return put(null).both(acknowledgement);
			}
		};
	}

	default SerialSupplier<T> getSupplier() {
		return new AbstractSerialSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				return take();
			}
		};
	}

}
