package io.datakernel.stream.stats;

import io.datakernel.async.Promise;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialConsumerFunction;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSupplierFunction;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerFunction;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.StreamSupplierFunction;
import io.datakernel.util.CollectionUtils;
import io.datakernel.util.IntrusiveLinkedList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static java.lang.System.currentTimeMillis;

public final class StreamRegistry<V> implements Iterable<V> {
	private final IntrusiveLinkedList<Entry<V>> list = new IntrusiveLinkedList<>();
	private int limit = 10;

	private static class Entry<T> {
		private final long timestamp;
		private final T operation;

		private Entry(T operation) {
			this.timestamp = currentTimeMillis();
			this.operation = operation;
		}

		@Override
		public String toString() {
			return operation + " " + (currentTimeMillis() - timestamp);
		}
	}

	public static <V> StreamRegistry<V> create() {
		return new StreamRegistry<>();
	}

	public StreamRegistry<V> withLimit(int limit) {
		this.limit = limit;
		return this;
	}

	public final class RegisterFunction<T> implements
			SerialSupplierFunction<T, SerialSupplier<T>>,
			SerialConsumerFunction<T, SerialConsumer<T>>,
			StreamSupplierFunction<T, StreamSupplier<T>>,
			StreamConsumerFunction<T, StreamConsumer<T>> {
		private final V value;

		private RegisterFunction(V value) {this.value = value;}

		@Override
		public StreamConsumer<T> apply(StreamConsumer<T> consumer) {
			return register(consumer, value);
		}

		@Override
		public StreamSupplier<T> apply(StreamSupplier<T> supplier) {
			return register(supplier, value);
		}

		@Override
		public SerialConsumer<T> apply(SerialConsumer<T> consumer) {
			return register(consumer, value);
		}

		@Override
		public SerialSupplier<T> apply(SerialSupplier<T> supplier) {
			return register(supplier, value);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> RegisterFunction<T> register(V value) {
		return new RegisterFunction<>(value);
	}

	public <T> SerialSupplier<T> register(SerialSupplier<T> supplier, V value) {
		return supplier.withEndOfStream(subscribe(value));
	}

	public <T> SerialConsumer<T> register(SerialConsumer<T> consumer, V value) {
		return consumer.withAcknowledgement(subscribe(value));
	}

	public <T> StreamConsumer<T> register(StreamConsumer<T> consumer, V value) {
		return consumer.withAcknowledgement(subscribe(value));
	}

	public <T> StreamSupplier<T> register(StreamSupplier<T> supplier, V value) {
		return supplier.withEndOfStream(subscribe(value));
	}

	private Function<Promise<Void>, Promise<Void>> subscribe(V value) {
		Entry<V> entry = new Entry<>(value);
		IntrusiveLinkedList.Node<Entry<V>> node = list.addFirstValue(entry);
		return promise -> promise
				.whenComplete(($, e) -> list.removeNode(node));
	}

	@Override
	public Iterator<V> iterator() {
		Iterator<Entry<V>> iterator = list.iterator();
		return new Iterator<V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public V next() {
				return iterator.next().operation;
			}
		};
	}

	@JmxAttribute(name = "")
	public String getString() {
		List<Entry> entries = new ArrayList<>();
		list.forEach(entries::add);
		return CollectionUtils.toLimitedString(entries, limit);
	}

}
