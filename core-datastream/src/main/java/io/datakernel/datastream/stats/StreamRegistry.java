package io.datakernel.datastream.stats;

import io.datakernel.common.collection.CollectionUtils;
import io.datakernel.common.collection.IntrusiveLinkedList;
import io.datakernel.common.collection.IntrusiveLinkedList.Node;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.dsl.ChannelConsumerTransformer;
import io.datakernel.csp.dsl.ChannelSupplierTransformer;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamConsumerTransformer;
import io.datakernel.datastream.processor.StreamSupplierTransformer;
import io.datakernel.jmx.api.attribute.JmxAttribute;
import io.datakernel.promise.Promise;

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

	public final class RegisterTransformer<T> implements
			ChannelSupplierTransformer<T, ChannelSupplier<T>>,
			ChannelConsumerTransformer<T, ChannelConsumer<T>>,
			StreamSupplierTransformer<T, StreamSupplier<T>>,
			StreamConsumerTransformer<T, StreamConsumer<T>> {
		private final V value;

		private RegisterTransformer(V value) {this.value = value;}

		@Override
		public StreamConsumer<T> transform(StreamConsumer<T> consumer) {
			return register(consumer, value);
		}

		@Override
		public StreamSupplier<T> transform(StreamSupplier<T> supplier) {
			return register(supplier, value);
		}

		@Override
		public ChannelConsumer<T> transform(ChannelConsumer<T> consumer) {
			return register(consumer, value);
		}

		@Override
		public ChannelSupplier<T> transform(ChannelSupplier<T> supplier) {
			return register(supplier, value);
		}
	}

	public <T> RegisterTransformer<T> register(V value) {
		return new RegisterTransformer<>(value);
	}

	public <T> ChannelSupplier<T> register(ChannelSupplier<T> supplier, V value) {
		return supplier.withEndOfStream(subscribe(value));
	}

	public <T> ChannelConsumer<T> register(ChannelConsumer<T> consumer, V value) {
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
		Node<Entry<V>> node = list.addFirstValue(entry);
		return promise -> promise
				.whenComplete(() -> list.removeNode(node));
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
		List<Entry<V>> entries = new ArrayList<>();
		list.forEach(entries::add);
		return CollectionUtils.toLimitedString(entries, limit);
	}

}
