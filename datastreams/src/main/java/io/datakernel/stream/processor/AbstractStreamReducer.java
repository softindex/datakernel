/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.stream.processor;

import com.google.common.base.Function;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.stream.AbstractStreamTransformer_N_1;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Perform aggregative functions on the elements from input streams. Searches key of item
 * with key function, selects elements with some key, reductions it and streams result sorted by key.
 * Elements from stream to input must be sorted by keys. It is {@link AbstractStreamTransformer_N_1}
 * because it represents few consumers and one producer.
 *
 * @param <K> type of key of element
 * @param <O> type of output data
 * @param <A> type of accumulator
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class AbstractStreamReducer<K, O, A> extends AbstractStreamTransformer_N_1<O> {
	public static final int DEFAULT_BUFFER_SIZE = 256;

	private int bufferSize = DEFAULT_BUFFER_SIZE;

	private InputConsumer<?> lastInput;
	private K key = null;
	private A accumulator;

	private final PriorityQueue<InputConsumer> priorityQueue;
	private int streamsAwaiting;
	private int streamsOpen;

	private int jmxInputItems;
	private int jmxOnFirst;
	private int jmxOnNext;
	private int jmxOnComplete;

	/**
	 * Creates a new instance of AbstractStreamReducer
	 *
	 * @param eventloop     eventloop in which runs reducer
	 * @param keyComparator comparator for compare keys
	 */
	public AbstractStreamReducer(Eventloop eventloop, final Comparator<K> keyComparator) {
		super(eventloop);
		this.outputProducer = new OutputProducer();
		this.priorityQueue = new PriorityQueue<>(1, new Comparator<InputConsumer>() {
			@Override
			public int compare(InputConsumer o1, InputConsumer o2) {
				int compare = ((Comparator) keyComparator).compare(o1.headKey, o2.headKey);
				if (compare != 0)
					return compare;
				return o1.index - o2.index;
			}
		});
	}

	protected AbstractStreamReducer<K, O, A> withBufferSize(int bufferSize) {
		checkArgument(bufferSize >= 0, "bufferSize must be positive value, got %s", bufferSize);
		this.bufferSize = bufferSize;
		return this;
	}

	protected <I> StreamConsumer<I> newInput(Function<I, K> keyFunction, StreamReducers.Reducer<K, I, O, A> reducer) {
		InputConsumer input = new InputConsumer<>(priorityQueue, keyFunction, reducer);
		addInput(input);
		streamsAwaiting++;
		streamsOpen++;
		return input;
	}

	private final class InputConsumer<I> extends AbstractInputConsumer<I> implements StreamDataReceiver<I> {
		private final int index = inputConsumers.size();

		private final PriorityQueue<InputConsumer> priorityQueue;

		private final ArrayDeque<I> deque = new ArrayDeque<>();
		private final Function<I, K> keyFunction;
		private final StreamReducers.Reducer<K, I, O, A> reducer;
		private K headKey;
		private I headItem;

		private InputConsumer(PriorityQueue<InputConsumer> priorityQueue, Function<I, K> keyFunction, StreamReducers.Reducer<K, I, O, A> reducer) {
			this.priorityQueue = priorityQueue;
			this.keyFunction = keyFunction;
			this.reducer = reducer;
		}

		/**
		 * Processes received item. Adds item to deque, if deque size is buffer size or it is last
		 * input begins to reduce streams
		 *
		 * @param item item to receive
		 */
		@Override
		public void onData(I item) {
			//noinspection AssertWithSideEffects
			assert jmxInputItems != ++jmxInputItems;
			if (headItem == null) {
				headItem = item;
				headKey = keyFunction.apply(headItem);
				priorityQueue.offer(this);
				streamsAwaiting--;
			} else {
				deque.offer(item);
			}
			if (deque.size() >= bufferSize) {
				suspend();
				outputProducer.produce();
			}
		}

		@Override
		public StreamDataReceiver<I> getDataReceiver() {
			return this;
		}

		@Override
		protected void onUpstreamEndOfStream() {
			streamsOpen--;
			if (headItem == null) {
				streamsAwaiting--;
			}
			outputProducer.produce();
		}
	}

	private final class OutputProducer extends AbstractOutputProducer {
		@Override
		protected void onDownstreamSuspended() {
		}

		@Override
		protected void onDownstreamResumed() {
			resumeProduce();
		}

		@Override
		protected void onDownstreamStarted() {
			resumeProduce();
		}

		@Override
		protected void doProduce() {
			while (isStatusReady() && streamsAwaiting == 0) {
				InputConsumer<Object> input = priorityQueue.poll();
				if (input == null)
					break;
				if (key != null && input.headKey.equals(key)) {
					assert jmxOnNext != ++jmxOnNext;
					accumulator = input.reducer.onNextItem(downstreamDataReceiver, key, input.headItem, accumulator);
				} else {
					if (lastInput != null) {
						assert jmxOnComplete != ++jmxOnComplete;
						lastInput.reducer.onComplete(downstreamDataReceiver, key, accumulator);
					}
					key = input.headKey;
					assert jmxOnFirst != ++jmxOnFirst;
					accumulator = input.reducer.onFirstItem(downstreamDataReceiver, key, input.headItem);
				}
				input.headItem = input.deque.poll();
				lastInput = input;
				if (input.headItem != null) {
					input.headKey = input.keyFunction.apply(input.headItem);
					priorityQueue.offer(input);
				} else {
					if (input.getConsumerStatus().isOpen()) {
						input.resume();
						streamsAwaiting++;
						break;
					}
				}
			}

			if (streamsOpen == 0 && priorityQueue.isEmpty()) {
				if (lastInput != null) {
					assert jmxOnComplete != ++jmxOnComplete;
					lastInput.reducer.onComplete(downstreamDataReceiver, key, accumulator);
					lastInput = null;
					key = null;
					accumulator = null;
				}
				sendEndOfStream();
			}
		}
	}

	@JmxAttribute
	public int getInputItems() {
		return jmxInputItems;
	}

	@JmxAttribute
	public int getOnFirst() {
		return jmxOnFirst;
	}

	@JmxAttribute
	public int getOnNext() {
		return jmxOnNext;
	}

	@JmxAttribute
	public int getOnComplete() {
		return jmxOnComplete;
	}

	@SuppressWarnings("AssertWithSideEffects")
	@Override
	public String toString() {
		String inputItems = "?";
		String next = "?";
		String first = "?";
		String complete = "?";
		assert (inputItems = "" + jmxInputItems) != null;
		assert (next = "" + jmxOnNext) != null;
		assert (first = "" + jmxOnFirst) != null;
		assert (complete = "" + jmxOnComplete) != null;

		return '{' + super.toString() +
				" items:" + inputItems +
				" onNext:" + next +
				" onFirst:" + first +
				" onComplete:" + complete +
				'}';
	}

}
