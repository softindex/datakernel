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
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.AbstractStreamTransformer_M_1;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Perform aggregative functions on the elements from input streams. Searches key of item
 * with key function, selects elements with some key, reductions it and streams result sorted by key.
 * Elements from stream to input must be sorted by keys. It is {@link AbstractStreamTransformer_M_1}
 * because it represents few consumers and one producer.
 *
 * @param <K> type of key of element
 * @param <O> type of output data
 * @param <A> type of accumulator
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class AbstractStreamReducer<K, O, A> extends AbstractStreamTransformer_M_1<O> implements AbstractStreamReducerMBean {
	public static final int BUFFER_SIZE = 1024;

	private final int bufferSize;

	private InternalConsumer<?> lastInput;
	private K key = null;
	private A accumulator;

	private final PriorityQueue<InternalConsumer> priorityQueue;
	private int streamsAwaiting;

	private int jmxInputItems;
	private int jmxOnFirst;
	private int jmxOnNext;
	private int jmxOnComplete;

	/**
	 * Creates a new instance of AbstractStreamReducer
	 *
	 * @param eventloop     eventloop in which runs reducer
	 * @param keyComparator comparator for compare keys
	 * @param bufferSize    maximal size of items which can be stored before reducing
	 */
	public AbstractStreamReducer(Eventloop eventloop, final Comparator<K> keyComparator, int bufferSize) {
		super(eventloop);
		checkArgument(bufferSize >= 0, "bufferSize must be positive value, got %s", bufferSize);
		this.bufferSize = bufferSize;
		this.priorityQueue = new PriorityQueue<>(1, new Comparator<InternalConsumer>() {
			@Override
			public int compare(InternalConsumer o1, InternalConsumer o2) {
				int compare = ((Comparator) keyComparator).compare(o1.headKey, o2.headKey);
				if (compare != 0)
					return compare;
				return o1.index - o2.index;
			}
		});
	}

	/**
	 * Creates a new instance of AbstractStreamReducer with default buffer size - 1024
	 *
	 * @param eventloop     eventloop in which runs reducer
	 * @param keyComparator comparator for compare keys
	 */
	public AbstractStreamReducer(Eventloop eventloop, Comparator<K> keyComparator) {
		this(eventloop, keyComparator, BUFFER_SIZE);
	}

	/**
	 * This method is called if consumer was changed, checks if it has consumers, if not sets status
	 * end of stream.
	 */
	@Override
	protected void onProducerStarted() {
		if (inputs.isEmpty()) {
			sendEndOfStream();
		}
	}

	@Override
	@SuppressWarnings("AssertWithSideEffects")
	protected void doProduce() {
		while (status == READY && streamsAwaiting == 0) {
			InternalConsumer<Object> input = priorityQueue.poll();
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
				if (input.getUpstreamStatus() < END_OF_STREAM) {
					streamsAwaiting++;
					break;
				}
			}
		}

		if (status == READY) {
			resumeAllUpstreams();
		}

		if (status == READY && priorityQueue.isEmpty() && streamsAwaiting == 0) {
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

	private class InternalConsumer<I> extends AbstractStreamConsumer<I> implements StreamDataReceiver<I> {
		private final int index = inputs.size();

		private final PriorityQueue<InternalConsumer> priorityQueue;

		private final ArrayDeque<I> deque = new ArrayDeque<>();
		private final Function<I, K> keyFunction;
		private final StreamReducers.Reducer<K, I, O, A> reducer;
		private K headKey;
		private I headItem;

		private InternalConsumer(Eventloop eventloop, PriorityQueue<InternalConsumer> priorityQueue, Function<I, K> keyFunction, StreamReducers.Reducer<K, I, O, A> reducer) {
			super(eventloop);
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
			if (deque.size() == bufferSize && streamsAwaiting == 0) {
				produce();
				if (status != READY) {
					suspendAllUpstreams();
				}
			}
		}

		@Override
		public void onProducerEndOfStream() {
			if (headItem == null) {
				streamsAwaiting--;
			}
			produce();
		}

		@Override
		public void onProducerError(Exception e) {
			upstreamProducer.onConsumerError(e);
			onConsumerError(e);
		}

		@Override
		public StreamDataReceiver<I> getDataReceiver() {
			return this;
		}
	}

	@Override
	public void onResumed() {
		resumeProduce();
	}

	protected <I> StreamConsumer<I> newInput(Function<I, K> keyFunction, StreamReducers.Reducer<K, I, O, A> reducer) {
		InternalConsumer input = new InternalConsumer<>(eventloop, priorityQueue, keyFunction, reducer);
		addInput(input);
		streamsAwaiting++;
		return input;
	}

	@Override
	public int getInputItems() {
		return jmxInputItems;
	}

	@Override
	public int getOnFirst() {
		return jmxOnFirst;
	}

	@Override
	public int getOnNext() {
		return jmxOnNext;
	}

	@Override
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
