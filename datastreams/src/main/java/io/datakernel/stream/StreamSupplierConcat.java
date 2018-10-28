package io.datakernel.stream;

import io.datakernel.async.Promise;

import java.util.Iterator;

/**
 * Represents {@link AbstractStreamTransformer_1_1}, which created with iterator with {@link AbstractStreamSupplier}
 * which will stream to this
 *
 * @param <T> type of received data
 */
class StreamSupplierConcat<T> extends AbstractStreamSupplier<T> {
	private final Iterator<StreamSupplier<T>> iterator;
	private StreamSupplier<T> supplier;
	private InternalConsumer internalConsumer;

	StreamSupplierConcat(Iterator<StreamSupplier<T>> iterator) {
		this.iterator = iterator;
	}

	private class InternalConsumer extends AbstractStreamConsumer<T> {
		@Override
		protected Promise<Void> onEndOfStream() {
			eventloop.post(() -> {
				supplier = null;
				internalConsumer = null;
				if (isReceiverReady()) {
					onProduce(getCurrentDataAcceptor());
				}
			});
			return StreamSupplierConcat.this.getConsumer().getAcknowledgement();
		}

		@Override
		protected void onError(Throwable t) {
			StreamSupplierConcat.this.close(t);
		}
	}

	@Override
	protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
		assert dataAcceptor != null;
		if (supplier == null) {
			if (!iterator.hasNext()) {
				eventloop.post(this::sendEndOfStream);
				return;
			}
			supplier = iterator.next();
			internalConsumer = new InternalConsumer();
			supplier.streamTo(internalConsumer);
		}
		supplier.resume(dataAcceptor);
	}

	@Override
	protected void onSuspended() {
		if (supplier != null) {
			supplier.suspend();
		}
	}

	@Override
	protected void onError(Throwable t) {
		if (supplier != null) {
			assert internalConsumer != null;
			internalConsumer.close(t);
		} else {
			// TODO ?
		}
	}

	@Override
	protected void cleanup() {
		supplier = null;
	}

}
