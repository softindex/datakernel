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
				return supplier.get()
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
				assert !isProcessComplete();
				return consumer.accept(item)
						.thenComposeEx(($, e) -> {
							if (isProcessComplete()) {
								return Stage.ofException(ASYNC_PROCESS_IS_COMPLETE);
							}
							if (e == null) {
								return Stage.complete();
							} else {
								closeWithError(e);
								return Stage.ofException(e);
							}
						});
			}

			@Override
			protected void onClosed(Throwable e) {
				consumer.closeWithError(e);
				closeWithError(e);
			}
		};
	}

	protected ByteBufsSupplier sanitize(ByteBufsSupplier supplier) {
		return new ByteBufsSupplier() {
			@Override
			public Stage<Void> needMoreData() {
				assert !isProcessComplete();
				return supplier.needMoreData()
						.thenComposeEx(($, e) -> {
							if (isProcessComplete()) {
								return Stage.ofException(ASYNC_PROCESS_IS_COMPLETE);
							}
							if (e == null) {
								return Stage.complete();
							} else {
								closeWithError(e);
								return Stage.ofException(e);
							}
						});
			}

			@Override
			public Stage<Void> endOfStream() {
				return supplier.endOfStream();
			}

			@Override
			public void closeWithError(Throwable e) {
				supplier.closeWithError(e);
				AbstractIOAsyncProcess.this.closeWithError(e);
			}
		};
	}

}
