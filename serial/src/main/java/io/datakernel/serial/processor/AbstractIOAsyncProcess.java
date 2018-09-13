package io.datakernel.serial.processor;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.async.Stage;
import io.datakernel.exception.StacklessException;
import io.datakernel.serial.*;

import static io.datakernel.util.Recyclable.deepRecycle;

public abstract class AbstractIOAsyncProcess extends AbstractAsyncProcess {
	public static final StacklessException ASYNC_PROCESS_IS_COMPLETE = new StacklessException("AsyncProcess is complete");

	protected <T> SerialSupplier<T> sanitize(SerialSupplier<T> supplier) {
		return new AbstractSerialSupplier<T>() {
			@Override
			public Stage<T> get() {
				assert !isProcessComplete();
				return handle(supplier.get());
			}

			@Override
			protected void onClosed(Throwable e) {
				supplier.closeWithError(e);
				AbstractIOAsyncProcess.this.closeWithError(e);
			}
		};
	}

	protected <T> SerialConsumer<T> sanitize(SerialConsumer<T> consumer) {
		return new AbstractSerialConsumer<T>() {
			@Override
			public Stage<Void> accept(@Nullable T item) {
				return handle(consumer.accept(item));
			}

			@Override
			protected void onClosed(Throwable e) {
				consumer.closeWithError(e);
				AbstractIOAsyncProcess.this.closeWithError(e);
			}
		};
	}

	protected ByteBufsSupplier sanitize(ByteBufsSupplier supplier) {
		return new ByteBufsSupplier() {
			@Override
			public Stage<Void> needMoreData() {
				return handle(supplier.needMoreData());
			}

			@Override
			public Stage<Void> endOfStream() {
				return handle(supplier.endOfStream());
			}

			@Override
			public void closeWithError(Throwable e) {
				supplier.closeWithError(e);
				AbstractIOAsyncProcess.this.closeWithError(e);
			}
		};
	}

	private <T> Stage<T> handle(Stage<T> stage) {
		assert !isProcessComplete();
		return stage
				.thenComposeEx((item, e) -> {
					if (isProcessComplete()) {
						deepRecycle(item);
						return Stage.ofException(ASYNC_PROCESS_IS_COMPLETE);
					}
					if (e == null) {
						return Stage.of(item);
					} else {
						closeWithError(e);
						return Stage.ofException(e);
					}
				});
	}

}
