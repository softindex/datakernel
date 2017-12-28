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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

import java.util.*;
import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkArgument;

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
public abstract class AbstractStreamReducer<K, O, A> implements HasOutput<O>, HasInputs {
	public static final int DEFAULT_BUFFER_SIZE = 256;

	private final Eventloop eventloop;
	private final List<Input> inputs = new ArrayList<>();
	private final Output output;

	private int bufferSize = DEFAULT_BUFFER_SIZE;

	private Input<?> lastInput;
	private K key = null;
	private A accumulator;

	private final PriorityQueue<Input> priorityQueue;
	private int streamsAwaiting;
	private int streamsOpen;

	/**
	 * Creates a new instance of AbstractStreamReducer
	 *
	 * @param eventloop     eventloop in which runs reducer
	 * @param keyComparator comparator for compare keys
	 */
	public AbstractStreamReducer(Eventloop eventloop, final Comparator<K> keyComparator) {
		this.eventloop = eventloop;
		this.output = new Output(eventloop);
		this.priorityQueue = new PriorityQueue<>(1, (o1, o2) -> {
			int compare = ((Comparator) keyComparator).compare(o1.headKey, o2.headKey);
			if (compare != 0)
				return compare;
			return o1.index - o2.index;
		});
	}

	protected AbstractStreamReducer<K, O, A> withBufferSize(int bufferSize) {
		checkArgument(bufferSize >= 0, "bufferSize must be positive value, got %s", bufferSize);
		this.bufferSize = bufferSize;
		return this;
	}

	protected <I> StreamConsumer<I> newInput(Function<I, K> keyFunction, StreamReducers.Reducer<K, I, O, A> reducer) {
		Input input = new Input<>(eventloop, inputs.size(), priorityQueue, keyFunction, reducer);
		inputs.add(input);
		streamsAwaiting++;
		streamsOpen++;
		return input;
	}

	@Override
	public List<? extends StreamConsumer<?>> getInputs() {
		return (List) inputs;
	}

	@Override
	public StreamProducer<O> getOutput() {
		return output;
	}

	private final class Input<I> extends AbstractStreamConsumer<I> implements StreamDataReceiver<I> {
		private final int index;

		private final PriorityQueue<Input> priorityQueue;

		private final ArrayDeque<I> deque = new ArrayDeque<>();
		private final Function<I, K> keyFunction;
		private final StreamReducers.Reducer<K, I, O, A> reducer;
		private K headKey;
		private I headItem;

		private Input(Eventloop eventloop, int index,
		              PriorityQueue<Input> priorityQueue, Function<I, K> keyFunction, StreamReducers.Reducer<K, I, O, A> reducer) {
			super(eventloop);
			this.index = index;
			this.priorityQueue = priorityQueue;
			this.keyFunction = keyFunction;
			this.reducer = reducer;
		}

		@Override
		protected void onWired() {
			super.onWired();
		}

		@Override
		protected void onStarted() {
			getProducer().produce(this);
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
			if (headItem == null) {
				headItem = item;
				headKey = keyFunction.apply(headItem);
				priorityQueue.offer(this);
				streamsAwaiting--;
			} else {
				deque.offer(item);
			}
			if (deque.size() >= bufferSize) {
				getProducer().suspend();
				produce();
			}
		}

		@Override
		protected void onEndOfStream() {
			streamsOpen--;
			if (headItem == null) {
				streamsAwaiting--;
			}
			produce();
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}
	}

	private final class Output extends AbstractStreamProducer<O> {
		protected Output(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onError(Throwable t) {
			inputs.forEach(input -> input.closeWithError(t));
		}

		@Override
		protected void produce() {
			AbstractStreamReducer.this.produce();
		}
	}

	private void produce() {
		StreamDataReceiver<O> dataReceiver = output.getCurrentDataReceiver();
		if (dataReceiver == null)
			return;
		while (streamsAwaiting == 0) {
			Input<Object> input = priorityQueue.poll();
			if (input == null)
				break;
			if (key != null && input.headKey.equals(key)) {
				accumulator = input.reducer.onNextItem(dataReceiver, key, input.headItem, accumulator);
			} else {
				if (lastInput != null) {
					lastInput.reducer.onComplete(dataReceiver, key, accumulator);
				}
				key = input.headKey;
				accumulator = input.reducer.onFirstItem(dataReceiver, key, input.headItem);
			}
			input.headItem = input.deque.poll();
			lastInput = input;
			if (input.headItem != null) {
				input.headKey = input.keyFunction.apply(input.headItem);
				priorityQueue.offer(input);
			} else {
				if (input.getStatus().isOpen()) {
					input.getProducer().produce(input);
					streamsAwaiting++;
					break;
				}
			}
		}

		if (streamsOpen == 0 && priorityQueue.isEmpty()) {
			if (lastInput != null) {
				lastInput.reducer.onComplete(dataReceiver, key, accumulator);
				lastInput = null;
				key = null;
				accumulator = null;
			}
			output.sendEndOfStream();
		}
	}


}
