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
import io.datakernel.stream.processor.Utils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.stream.AbstractStreamConsumer.*;
import static io.datakernel.stream.AbstractStreamProducer.*;
import static io.datakernel.stream.processor.Utils.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamRewiringWithStatus {
	private NioEventloop eventloop;
	private List<Integer> list;

	private StreamConsumer<Integer> readyConsumer;
	private StreamConsumer<Integer> suspendConsumer;
	private StreamConsumer<Integer> endConsumer;
	private StreamConsumer<Integer> closedConsumer;
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
		closedConsumer = new TestConsumerOneByOne(eventloop) {{close();}};
		closedWithErrorConsumer = new TestConsumerOneByOne(eventloop) {{closeWithError(new Exception());}};

		readyProducer = new TestProducerOfIterator(eventloop, it);
		suspendProducer = new TestProducerOfIterator(eventloop, it) {{onConsumerSuspended();}};
		endProducer = new TestProducerOfIterator(eventloop, it) {{sendEndOfStream();}};
		closedWithErrorProducer = new TestProducerOfIterator(eventloop, it) {{closeWithError(new Exception());}};
	}

	@Test
	// producer READY
	// consumer READY
	public void testProducerReadyConsumerReady() {
		readyProducer.streamTo(readyConsumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5), list);
		assertStatus(StreamProducerStatus.END_OF_STREAM, readyProducer);
		assertStatus(StreamConsumerStatus.CLOSED, readyConsumer);
	}

	@Test
	// producer READY
	// consumer SUSPEND
	public void testProducerReadyConsumerSuspend() {
		readyProducer.streamTo(suspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		// TODO (vsavchuk): why are both SUSPENDED? please check and fix others too
		assertStatus(StreamProducerStatus.SUSPENDED, readyProducer);
		assertStatus(StreamConsumerStatus.SUSPENDED, suspendConsumer);
	}

	@Test
	// producer READY
	// consumer END_OF_STREAM
	public void testProducerReadyConsumerEndOfStream() {
		readyProducer.streamTo(endConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) readyProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) endConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer READY
	// consumer CLOSED
	public void testProducerReadyConsumerClosed() {
		readyProducer.streamTo(closedConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) readyProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) closedConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer READY
	// consumer CLOSED_WITH_ERROR
	public void testProducerReadyConsumerClosedWithError() {
		readyProducer.streamTo(closedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) readyProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) closedWithErrorConsumer).getStatus() == CLOSED_WITH_ERROR);
	}

	//------------------------------------------------------------------------------------------------------------------

	@Test
	// producer SUSPEND
	// consumer READY
	public void testProducerSuspendConsumerReady() {
		suspendProducer.streamTo(readyConsumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5), list);
//		assertTrue(((AbstractStreamProducer) suspendProducer).getStatus() == END_OF_STREAM);
//		assertTrue(((AbstractStreamConsumer) readyConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer SUSPEND
	// consumer SUSPEND
	public void testProducerSuspendConsumerSuspend() {
		suspendProducer.streamTo(suspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) suspendProducer).getStatus() == SUSPENDED);
//		assertTrue(((AbstractStreamConsumer) suspendConsumer).getStatus() == SUSPENDED);
	}

	@Test
	// producer SUSPEND
	// consumer END_OF_STREAM
	public void testProducerSuspendConsumerEndOfStream() {
		suspendProducer.streamTo(endConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) suspendProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) endConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer SUSPEND
	// consumer CLOSED
	public void testProducerSuspendConsumerClosed() {
		suspendProducer.streamTo(closedConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) suspendProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) closedConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer SUSPEND
	// consumer CLOSED_WITH_ERROR
	public void testProducerSuspendConsumerClosedWithError() {
		suspendProducer.streamTo(closedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) suspendProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) closedWithErrorConsumer).getStatus() == CLOSED_WITH_ERROR);
	}

	//------------------------------------------------------------------------------------------------------------------

	@Test
	// producer END_OF_STREAM
	// consumer READY
	public void testProducerEndOfStreamConsumerReady() {
		endProducer.streamTo(readyConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) endProducer).getStatus() == END_OF_STREAM);
//		assertTrue(((AbstractStreamConsumer) readyConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer END_OF_STREAM
	// consumer SUSPEND
	public void testProducerEndOfStreamConsumerSuspend() {
		endProducer.streamTo(suspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) endProducer).getStatus() == END_OF_STREAM);
//		assertTrue(((AbstractStreamConsumer) suspendConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer END_OF_STREAM
	// consumer END_OF_STREAM
	public void testProducerEndOfStreamConsumerEndOfStream() {
		endProducer.streamTo(endConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) endProducer).getStatus() == END_OF_STREAM);
//		assertTrue(((AbstractStreamConsumer) endConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer END_OF_STREAM
	// consumer CLOSED
	public void testProducerEndOfStreamConsumerCLOSED() {
		endProducer.streamTo(closedConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) endProducer).getStatus() == END_OF_STREAM);
//		assertTrue(((AbstractStreamConsumer) closedConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer END_OF_STREAM
	// consumer CLOSED_WITH_ERROR
	public void testProducerEndOfStreamConsumerClosedWithError() {
		endProducer.streamTo(closedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) endProducer).getStatus() == END_OF_STREAM);
//		assertTrue(((AbstractStreamConsumer) closedWithErrorConsumer).getStatus() == CLOSED_WITH_ERROR);
	}

	//------------------------------------------------------------------------------------------------------------------

	@Test
	// producer CLOSED_WITH_ERROR
	// consumer READY
	public void testCloseWithErrorConsumerReady() {
		closedWithErrorProducer.streamTo(readyConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) closedWithErrorProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) readyConsumer).getStatus() == CLOSED_WITH_ERROR);
	}

	@Test
	// producer CLOSED_WITH_ERROR
	// consumer SUSPEND
	public void testCloseWithErrorConsumerSuspend() {
		closedWithErrorProducer.streamTo(suspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) closedWithErrorProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) suspendConsumer).getStatus() == CLOSED_WITH_ERROR);
	}

	@Test
	// producer CLOSED_WITH_ERROR
	// consumer END_OF_STREAM
	public void testCloseWithErrorConsumerEndOfStream() {
		closedWithErrorProducer.streamTo(endConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) closedWithErrorProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) endConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer CLOSED_WITH_ERROR
	// consumer CLOSED
	public void testCloseWithErrorConsumerCLOSED() {
		closedWithErrorProducer.streamTo(closedConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) closedWithErrorProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) closedConsumer).getStatus() == CLOSED);
	}

	@Test
	// producer CLOSED_WITH_ERROR
	// consumer CLOSED_WITH_ERROR
	public void testCloseWithErrorConsumerClosedWithError() {
		closedWithErrorProducer.streamTo(closedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
//		assertTrue(((AbstractStreamProducer) closedWithErrorProducer).getStatus() == CLOSED_WITH_ERROR);
//		assertTrue(((AbstractStreamConsumer) closedWithErrorConsumer).getStatus() == CLOSED_WITH_ERROR);
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
			close();
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
