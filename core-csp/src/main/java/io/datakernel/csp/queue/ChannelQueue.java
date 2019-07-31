package io.datakernel.csp.queue;

import io.datakernel.async.Cancellable;
import io.datakernel.async.Promise;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.AbstractChannelSupplier;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a queue of elements, which you can {@code put}
 * or {@code take} to pass from {@link ChannelConsumer} to
 * {@link ChannelSupplier}.
 *
 * @param <T> type of values stored in the queue
 */
public interface ChannelQueue<T> extends Cancellable {
	/**
	 * Puts a value in the queue and returns a
	 * {@code promise} of {@code null} as a marker of completion.
	 *
	 * @param value a value passed to the queue
	 * @return {@code promise} of {@code null} as a marker of completion
	 */
	Promise<Void> put(@Nullable T value);

	/**
	 * Takes an element of this queue and wraps it in {@code promise}.
	 *
	 * @return a {@code promise} of value from the queue
	 */
	Promise<T> take();

	/**
	 * Returns a {@code ChannelConsumer} which puts value in
	 * this queue when {@code accept(T value)} is called.
	 *
	 * @return a {@code ChannelConsumer} for this queue
	 */
	default ChannelConsumer<T> getConsumer() {
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				return put(value);
			}
		};
	}

	/**
	 * Returns a {@code ChannelConsumer} which puts non-null
	 * value in this queue when {@code accept(T value)} is
	 * called. Otherwise, puts {@code null} and waits for the
	 * {@code acknowledgement} completion.
	 * <p>
	 * This method is useful if you need to control the situations
	 * when there are no more elements to be accepted (for example,
	 * get a {@code ChannelSupplier} in such case).
	 *
	 * @param acknowledgement a promise which will work when
	 *                           a {@code null} value is passed
	 * @return a ChannelConsumer with custom behaviour in case a
	 * {@code null} value is accepted
	 */
	default ChannelConsumer<T> getConsumer(Promise<Void> acknowledgement) {
		return new AbstractChannelConsumer<T>(this) {
			@Override
			protected Promise<Void> doAccept(T value) {
				if (value != null) return put(value);
				return put(null).both(acknowledgement);
			}
		};
	}

	/**
	 * Returns a {@link ChannelSupplier} which gets value from this
	 * queue wrapped in {@code Promise} when {@code get()} is called.
	 *
	 * @return a ChannelSupplier which takes values from this queue
	 */
	default ChannelSupplier<T> getSupplier() {
		return new AbstractChannelSupplier<T>(this) {
			@Override
			protected Promise<T> doGet() {
				return take();
			}
		};
	}

}
