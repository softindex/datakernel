package io.datakernel.stream.stats;

import io.datakernel.jmx.JmxAttribute;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamModifier;
import io.datakernel.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.currentTimeMillis;

public class StreamOps {
	private final List<Entry> list = new ArrayList<>();

	private static class Entry {
		private final long timestamp;
		private final Object operation;

		private Entry(Object operation) {
			this.timestamp = currentTimeMillis();
			this.operation = operation;
		}

		@Override
		public String toString() {
			return operation + " " + (currentTimeMillis() - timestamp);
		}
	}

	public static StreamOps create() {
		return new StreamOps();
	}

	@SuppressWarnings("unchecked")
	public <T> StreamModifier<T, T> newEntry(Object operation) {
		return new StreamModifier<T, T>() {
			@Override
			public StreamConsumer<T> apply(StreamConsumer<T> consumer) {
				return newEntry(consumer, operation);
			}

			@Override
			public StreamProducer<T> apply(StreamProducer<T> producer) {
				return newEntry(producer, operation);
			}
		};
	}

	public <T> StreamConsumer<T> newEntry(StreamConsumer<T> consumer, Object operation) {
		Entry entry = new Entry(operation);
		list.add(0, entry);
		consumer.getEndOfStream().whenComplete(($, throwable) -> list.remove(entry));
		return consumer;
	}

	public <T> StreamProducer<T> newEntry(StreamProducer<T> producer, Object operation) {
		Entry entry = new Entry(operation);
		list.add(0, entry);
		producer.getEndOfStream().whenComplete(($, throwable) -> list.remove(entry));
		return producer;
	}

	@JmxAttribute(name = "")
	public String get() {
		return CollectionUtils.toLimitedString(list, 10);
	}
}
