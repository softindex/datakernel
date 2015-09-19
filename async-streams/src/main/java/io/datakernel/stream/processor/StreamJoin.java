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
import io.datakernel.stream.*;

import java.util.ArrayDeque;
import java.util.Comparator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a object which has left and right consumers, and one producer. After receiving data
 * it can join it, available are inner join and left join. It work analogous joins from SQL.
 * It is a {@link AbstractStreamTransformer_M_1} which receives specified type and streams
 * set of join's result  to the destination .
 *
 * @param <K> type of  keys
 * @param <L> type of data from left stream
 * @param <R> type of data from right stream
 * @param <V> type of output data
 */
public final class StreamJoin<K, L, R, V> extends AbstractStreamTransformer_M_1<V> {

	/**
	 * It is primary interface of joiner. It contains methods which will join streams
	 *
	 * @param <K> type of  keys
	 * @param <L> type of data from left stream
	 * @param <R> type of data from right stream
	 * @param <V> type of output data
	 */
	public interface Joiner<K, L, R, V> {
		/**
		 * Streams objects with all fields from  both received streams as long as there is a match
		 * between the keys in both items.
		 *
		 * @param key    on this key it will join
		 * @param left   left stream
		 * @param right  right stream
		 * @param output callback for sending result
		 */
		void onInnerJoin(K key, L left, R right, StreamDataReceiver<V> output);

		/**
		 * Streams objects with all fields from the left stream , with the matching key - fields in the
		 * right stream. The field of result object is NULL in the right stream when there is no match.
		 *
		 * @param key    on this key it will join
		 * @param left   left stream
		 * @param output callback for sending result
		 */
		void onLeftJoin(K key, L left, StreamDataReceiver<V> output);
	}

	/**
	 * Represent joiner which produce only inner joins
	 *
	 * @param <K> type of  keys
	 * @param <L> type of data from left stream
	 * @param <R> type of data from right stream
	 * @param <V> type of output data
	 */
	public static abstract class InnerJoiner<K, L, R, V> implements Joiner<K, L, R, V> {
		/**
		 * Left join does nothing for absence null fields in result inner join
		 *
		 * @param key    on this key it will join
		 * @param left   left stream
		 * @param output callback for sending result
		 */
		@Override
		public void onLeftJoin(K key, L left, StreamDataReceiver<V> output) {
		}
	}

	/**
	 * Simple implementation of Joiner, which does inner and left join
	 *
	 * @param <K> type of  keys
	 * @param <L> type of data from left stream
	 * @param <R> type of data from right stream
	 * @param <V> type of output data
	 */
	public static abstract class ValueJoiner<K, L, R, V> implements Joiner<K, L, R, V> {
		/**
		 * Method which contains realization inner join.
		 *
		 * @param key   on this key it will join
		 * @param left  left stream
		 * @param right right stream
		 * @return stream with joined streams
		 */
		public abstract V doInnerJoin(K key, L left, R right);

		/**
		 * Method which contains realization left join
		 *
		 * @param key  on this key it will join
		 * @param left left stream
		 * @return stream with joined streams
		 */
		public V doLeftJoin(K key, L left) {
			return null;
		}

		@Override
		public final void onInnerJoin(K key, L left, R right, StreamDataReceiver<V> output) {
			V result = doInnerJoin(key, left, right);
			if (result != null) {
				output.onData(result);
			}
		}

		@Override
		public final void onLeftJoin(K key, L left, StreamDataReceiver<V> output) {
			V result = doLeftJoin(key, left);
			if (result != null) {
				output.onData(result);
			}
		}
	}

	private final Comparator<K> keyComparator;
	private final InternalConsumer<L> left;
	private final InternalConsumer<R> right;

	private final ArrayDeque<L> leftDeque = new ArrayDeque<>();
	private final ArrayDeque<R> rightDeque = new ArrayDeque<>();

	private final Function<L, K> leftKeyFunction;
	private final Function<R, K> rightKeyFunction;

	private final Joiner<K, L, R, V> joiner;

	/**
	 * Creates a new instance of StreamJoin
	 *
	 * @param eventloop        eventloop in which runs reducer
	 * @param keyComparator    comparator for compare keys
	 * @param leftKeyFunction  function for counting keys of left stream
	 * @param rightKeyFunction function for counting keys of right stream
	 * @param joiner           joiner which will join streams
	 */
	public StreamJoin(Eventloop eventloop, Comparator<K> keyComparator,
	                  Function<L, K> leftKeyFunction, Function<R, K> rightKeyFunction,
	                  Joiner<K, L, R, V> joiner) {
		super(eventloop);
		this.keyComparator = checkNotNull(keyComparator);
		this.joiner = checkNotNull(joiner);
		this.left = addInput(new InternalConsumer<>(eventloop, leftDeque));
		this.right = addInput(new InternalConsumer<>(eventloop, rightDeque));
		this.leftKeyFunction = checkNotNull(leftKeyFunction);
		this.rightKeyFunction = checkNotNull(rightKeyFunction);
	}

	@Override
	protected void doProduce() {
		if (internalProducer.getStatus() == AbstractStreamProducer.READY && !leftDeque.isEmpty() && !rightDeque.isEmpty()) {
			L leftValue = leftDeque.peek();
			K leftKey = leftKeyFunction.apply(leftValue);
			R rightValue = rightDeque.peek();
			K rightKey = rightKeyFunction.apply(rightValue);
			for (; ; ) {
				int compare = keyComparator.compare(leftKey, rightKey);
				if (compare < 0) {
					leftDeque.poll();
					if (leftDeque.isEmpty())
						break;
					leftValue = leftDeque.peek();
					leftKey = leftKeyFunction.apply(leftValue);
				} else if (compare > 0) {
					joiner.onLeftJoin(leftKey, leftValue, downstreamDataReceiver);
					rightDeque.poll();
					if (rightDeque.isEmpty())
						break;
					rightValue = rightDeque.peek();
					rightKey = rightKeyFunction.apply(rightValue);
				} else {
					joiner.onInnerJoin(leftKey, leftValue, rightValue, downstreamDataReceiver);
					leftDeque.poll();
					if (leftDeque.isEmpty())
						break;
					if (status != READY)
						break;
					leftValue = leftDeque.peek();
					leftKey = leftKeyFunction.apply(leftValue);
				}
			}
		}
		if (status == AbstractStreamProducer.READY) {
			if (left.getUpstreamStatus() == AbstractStreamProducer.END_OF_STREAM
					&& right.getUpstreamStatus() == AbstractStreamProducer.END_OF_STREAM) {
				sendEndOfStream();
			} else {
				resumeAllUpstreams();
			}
		}
	}

	@Override
	protected void onConsumerSuspended() {
		suspendAllUpstreams();
	}

	@Override
	protected void onResumed() {
		resumeProduce();
	}

	/**
	 * Returns left stream
	 */
	public StreamConsumer<L> getLeft() {
		return left;
	}

	/**
	 * Returns right stream
	 */
	public StreamConsumer<R> getRight() {
		return right;
	}

	private final class InternalConsumer<I> extends AbstractStreamConsumer<I> implements StreamDataReceiver<I> {
		private final ArrayDeque<I> deque;

		public InternalConsumer(Eventloop eventloop, ArrayDeque<I> deque) {
			super(eventloop);
			this.deque = checkNotNull(deque);
		}

		@Override
		public void onData(I item) {
			deque.add(item);
			produce();
		}

		@Override
		public void onProducerEndOfStream() {
			produce();
		}

		@Override
		public void onProducerError(Exception e) {
//			upstreamProducer.onConsumerError(e);
//			onConsumerError(e);
			super.onProducerError(e);
//			StreamJoin.this.onClosedWithError(e);
			internalProducer.getDownstream().onProducerError(e);
		}

		@Override
		public StreamDataReceiver<I> getDataReceiver() {
			return this;
		}
	}

}
