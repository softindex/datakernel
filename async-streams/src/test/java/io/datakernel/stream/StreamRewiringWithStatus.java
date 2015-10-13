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
import io.datakernel.eventloop.NioEventloop;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.stream.StreamStatus.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamRewiringWithStatus {
	private NioEventloop eventloop;
	private List<Integer> list;

	private StreamConsumer<Integer> readyConsumer;
	private StreamConsumer<Integer> suspendConsumer;
	private StreamConsumer<Integer> endConsumer;
	private StreamConsumer<Integer> closedWithErrorConsumer;

	private StreamProducer<Integer> readyProducer;
	private StreamProducer<Integer> suspendProducer;
	private StreamProducer<Integer> endProducer;
	private StreamProducer<Integer> closedWithErrorProducer;

	@Before
	public void before() {
		Iterator<Integer> it = asList(1, 2, 3, 4, 5).iterator();
		eventloop = new NioEventloop();
		list = new ArrayList<>();

		readyConsumer = new TestConsumerOneByOne(eventloop);
		suspendConsumer = new TestConsumerOneByOne(eventloop) {{suspend();}};
		endConsumer = new TestConsumerOneByOne(eventloop) {{onProducerEndOfStream();}};
		closedWithErrorConsumer = new TestConsumerOneByOne(eventloop) {{closeWithError(new Exception());}};

		readyProducer = new TestProducerOfIterator(eventloop, it);
		suspendProducer = new TestProducerOfIterator(eventloop, it) {{onConsumerSuspended();}};
		endProducer = new TestProducerOfIterator(eventloop, it) {{sendEndOfStream();}};
		closedWithErrorProducer = new TestProducerOfIterator(eventloop, it) {{closeWithError(new Exception());}};
	}

	@Test
	public void testStartProducerReadyConsumerReady() {
		readyProducer.streamTo(readyConsumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5), list);
		assertEquals(END_OF_STREAM, readyProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, readyConsumer.getConsumerStatus());
	}

	@Test
	public void testStartProducerReadyConsumerSuspend() {
		readyProducer.streamTo(suspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(SUSPENDED, readyProducer.getProducerStatus());
		assertEquals(SUSPENDED, suspendConsumer.getConsumerStatus());
	}

	@Test
	public void testStartProducerReadyConsumerEndOfStream() {
		readyProducer.streamTo(endConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, readyProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, endConsumer.getConsumerStatus());
	}

	@Test
	public void testStartProducerReadyConsumerClosedWithError() {
		readyProducer.streamTo(closedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, readyProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, closedWithErrorConsumer.getConsumerStatus());
	}

	//------------------------------------------------------------------------------------------------------------------

	@Test
	public void testStartProducerSuspendConsumerReady() {
		suspendProducer.streamTo(readyConsumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5), list);
		assertEquals(END_OF_STREAM, suspendProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, readyConsumer.getConsumerStatus());
	}

	@Test
	public void testStartProducerSuspendConsumerSuspend() {
		suspendProducer.streamTo(suspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(SUSPENDED, suspendProducer.getProducerStatus());
		assertEquals(SUSPENDED, suspendConsumer.getConsumerStatus());
	}

	@Test
	public void testStartProducerSuspendConsumerEndOfStream() {
		suspendProducer.streamTo(endConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, suspendProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, endConsumer.getConsumerStatus());
	}

	@Test
	public void testStartProducerSuspendConsumerClosedWithError() {
		suspendProducer.streamTo(closedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, suspendProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, closedWithErrorConsumer.getConsumerStatus());
	}

	//------------------------------------------------------------------------------------------------------------------

	@Test
	public void testStartProducerEndOfStreamConsumerReady() {
		endProducer.streamTo(readyConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(END_OF_STREAM, endProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, readyConsumer.getConsumerStatus());
	}

	@Test
	public void testStartProducerEndOfStreamConsumerSuspend() {
		endProducer.streamTo(suspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(END_OF_STREAM, endProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, suspendConsumer.getConsumerStatus());
	}

	@Test
	public void testStartProducerEndOfStreamConsumerEndOfStream() {
		endProducer.streamTo(endConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(END_OF_STREAM, endProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, endConsumer.getConsumerStatus());
	}

	@Test
	public void testStartProducerEndOfStreamConsumerClosedWithError() {
		endProducer.streamTo(closedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(END_OF_STREAM, endProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, closedWithErrorConsumer.getConsumerStatus());
	}

	//------------------------------------------------------------------------------------------------------------------

	@Test
	public void testStartCloseWithErrorConsumerReady() {
		closedWithErrorProducer.streamTo(readyConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, closedWithErrorProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, readyConsumer.getConsumerStatus());
	}

	@Test
	public void testStartCloseWithErrorConsumerSuspend() {
		closedWithErrorProducer.streamTo(suspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, closedWithErrorProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, suspendConsumer.getConsumerStatus());
	}

	@Test
	public void testStartCloseWithErrorConsumerEndOfStream() {
		closedWithErrorProducer.streamTo(endConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, closedWithErrorProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, endConsumer.getConsumerStatus());
	}

	@Test
	public void testStartCloseWithErrorConsumerClosedWithError() {
		closedWithErrorProducer.streamTo(closedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, closedWithErrorProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, closedWithErrorConsumer.getConsumerStatus());
	}

	//------------------------------------------------------------------------------------------------------------------

	private class TestConsumerOneByOne extends AbstractStreamConsumer<Integer> {

		protected TestConsumerOneByOne(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onStarted() {

		}

		@Override
		protected void onEndOfStream() {

		}

		@Override
		protected void onError(Exception e) {

		}

		@Override
		public StreamDataReceiver<Integer> getDataReceiver() {
			return new StreamDataReceiver<Integer>() {
				@Override
				public void onData(Integer item) {
					list.add(item);
					suspend();
					eventloop.post(new Runnable() {
						@Override
						public void run() {
							resume();
						}
					});
				}
			};
		}
	}

	private class TestProducerOfIterator extends AbstractStreamProducer<Integer> {
		private final Iterator<Integer> iterator;
		private boolean sendEndOfStream = true;

		public TestProducerOfIterator(Eventloop eventloop, Iterator<Integer> iterator) {
			this(eventloop, iterator, true);
		}

		public TestProducerOfIterator(Eventloop eventloop, Iterator<Integer> iterator, boolean sendEndOfStream) {
			super(eventloop);
			this.iterator = checkNotNull(iterator);
			this.sendEndOfStream = sendEndOfStream;
		}

		@Override
		protected void doProduce() {
			for (; ; ) {
				if (!iterator.hasNext())
					break;
				if (!isStatusReady())
					return;
				Integer item = iterator.next();
				send(item);
			}
			if (sendEndOfStream)
				sendEndOfStream();
		}

		@Override
		protected void onStarted() {
			produce();
		}

		@Override
		protected void onDataReceiverChanged() {

		}

		@Override
		protected void onSuspended() {

		}

		@Override
		protected void onResumed() {
			resumeProduce();
		}

		@Override
		protected void onError(Exception e) {

		}
	}
}
