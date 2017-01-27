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

package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.ExceptionStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers.JmxReducerSum;
import io.datakernel.stream.processor.StreamTransformer;

/**
 * Represent {@link StreamProducer} and {@link StreamConsumer} in the one object.
 * This object can receive and send streams of data.
 *
 * @param <I> type of input data for consumer
 * @param <O> type of output data of producer
 */
@SuppressWarnings("unchecked")
public abstract class AbstractStreamTransformer_1_1<I, O> implements StreamTransformer<I, O> {
	protected final Eventloop eventloop;

	protected Object tag;

	protected Inspector inspector;

	public interface Inspector {
		void onStarted();

		void onEndOfStream();

		void onError(Exception e);

		void onSuspended();

		void onResumed();
	}

	public static class JmxInspector implements Inspector {
		private int started;
		private int endOfStream;
		private ExceptionStats errors = ExceptionStats.create();
		private long suspended;
		private long resumed;

		@Override
		public void onStarted() {
			started++;
		}

		@Override
		public void onEndOfStream() {
			endOfStream++;
		}

		@Override
		public void onError(Exception e) {
			errors.recordException(e);
		}

		@Override
		public void onSuspended() {
			suspended++;
		}

		@Override
		public void onResumed() {
			resumed++;
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public int getStarted() {
			return started;
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public int getEndOfStream() {
			return endOfStream;
		}

		@JmxAttribute
		public ExceptionStats getErrors() {
			return errors;
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public long getSuspended() {
			return suspended;
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public long getResumed() {
			return resumed;
		}
	}

	protected abstract class AbstractInputConsumer extends AbstractStreamConsumer<I> {
		protected StreamDataReceiver<O> downstreamDataReceiver;

		public AbstractInputConsumer() {
			super(AbstractStreamTransformer_1_1.this.eventloop);
		}

		@Override
		protected final void onStarted() {
			if (inspector != null) inspector.onStarted();
			onUpstreamStarted();
		}

		protected void onUpstreamStarted() {
		}

		@Override
		protected final void onEndOfStream() {
			if (inspector != null) inspector.onEndOfStream();
			onUpstreamEndOfStream();
		}

		protected abstract void onUpstreamEndOfStream();

		@Override
		protected void onError(Exception e) {
			if (inspector != null) inspector.onError(e);
			getOutputImpl().closeWithError(e);
		}

		@Override
		public void suspend() {
			super.suspend();
		}

		@Override
		public void resume() {
			super.resume();
		}

		@Override
		public void closeWithError(Exception e) {
			super.closeWithError(e);
		}
	}

	protected abstract class AbstractOutputProducer extends AbstractStreamProducer<O> {
		public AbstractOutputProducer() {
			super(AbstractStreamTransformer_1_1.this.eventloop);
		}

		@Override
		protected final void onDataReceiverChanged() {
			AbstractInputConsumer inputConsumer = getInputImpl();
			inputConsumer.downstreamDataReceiver = this.downstreamDataReceiver;
			if (inputConsumer.getUpstream() != null) {
				inputConsumer.getUpstream().bindDataReceiver();
			}
		}

		@Override
		protected final void onStarted() {
			getInputImpl().bindUpstream();
			onDownstreamStarted();
		}

		protected void onDownstreamStarted() {
		}

		@Override
		protected void onError(Exception e) {
			getInputImpl().closeWithError(e);
		}

		@Override
		protected final void onSuspended() {
			if (inspector != null) inspector.onSuspended();
			onDownstreamSuspended();
		}

		protected abstract void onDownstreamSuspended();

		@Override
		protected final void onResumed() {
			if (inspector != null) inspector.onResumed();
			onDownstreamResumed();
		}

		protected abstract void onDownstreamResumed();
	}

	protected AbstractStreamTransformer_1_1(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	protected abstract AbstractInputConsumer getInputImpl();

	protected abstract AbstractOutputProducer getOutputImpl();

	@Override
	public final StreamConsumer<I> getInput() {
		return getInputImpl();
	}

	@Override
	public final StreamProducer<O> getOutput() {
		return getOutputImpl();
	}

	public void setTag(Object tag) {
		this.tag = tag;
	}

	@Override
	public String toString() {
		return tag != null ? tag.toString() : super.toString();
	}

}
