package io.datakernel.csp.queue;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.AbstractChannelSupplier;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;

public interface ChannelQueue<T> extends Cancellable {
	Promise<Void> put(@Nullable T value);

	Promise<T> take();

	default ChannelConsumer<T> getConsumer() {
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				return put(value);
			}
		};
	}

	default ChannelConsumer<T> getConsumer(MaterializedPromise<Void> acknowledgement) {
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				if (value != null) return put(value);
				return put(null).both(acknowledgement);
			}
		};
	}

	default ChannelSupplier<T> getSupplier() {
		return new AbstractChannelSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				return take();
			}
		};
	}

}
