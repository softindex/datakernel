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
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamStatus.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamRewiringWithStatus {
	private Eventloop eventloop;
	private List<Integer> list;
	private Iterator<Integer> it;

	@Before
	public void before() {
		it = asList(1, 2, 3, 4, 5).iterator();
		eventloop = Eventloop.create();
		list = new ArrayList<>();
	}

	@Test
	public void testStartProducerReadyConsumerReady() {
		StreamConsumer<Integer> startStatusReadyConsumer = new TestConsumerOneByOne(eventloop);
		StreamProducer<Integer> startStatusReadyProducer = new TestProducerOfIterator(eventloop, it);
		startStatusReadyProducer.streamTo(startStatusReadyConsumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5), list);
		assertEquals(END_OF_STREAM, startStatusReadyProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, startStatusReadyConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartProducerReadyConsumerSuspend() {
		StreamConsumer<Integer> startStatusSuspendConsumer = new TestConsumerOneByOne(eventloop) {{
			suspend();
		}};
		StreamProducer<Integer> startStatusReadyProducer = new TestProducerOfIterator(eventloop, it);
		startStatusReadyProducer.streamTo(startStatusSuspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(SUSPENDED, startStatusReadyProducer.getProducerStatus());
		assertEquals(SUSPENDED, startStatusSuspendConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartProducerReadyConsumerEndOfStream() {
		StreamConsumer<Integer> startStatusEndConsumer = new TestConsumerOneByOne(eventloop) {{
			onProducerEndOfStream();
		}};
		StreamProducer<Integer> startStatusReadyProducer = new TestProducerOfIterator(eventloop, it);
		startStatusReadyProducer.streamTo(startStatusEndConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, startStatusReadyProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, startStatusEndConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartProducerReadyConsumerClosedWithError() {
		StreamConsumer<Integer> startStatusClosedWithErrorConsumer = new TestConsumerOneByOne(eventloop) {{
			closeWithError(new Exception());
		}};
		StreamProducer<Integer> startStatusReadyProducer = new TestProducerOfIterator(eventloop, it);
		startStatusReadyProducer.streamTo(startStatusClosedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, startStatusReadyProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, startStatusClosedWithErrorConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	//------------------------------------------------------------------------------------------------------------------

	@Test
	public void testStartProducerSuspendConsumerReady() {
		StreamConsumer<Integer> startStatusReadyConsumer = new TestConsumerOneByOne(eventloop);
		StreamProducer<Integer> startStatusSuspendProducer = new TestProducerOfIterator(eventloop, it) {{
			onConsumerSuspended();
		}};
		startStatusSuspendProducer.streamTo(startStatusReadyConsumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5), list);
		assertEquals(END_OF_STREAM, startStatusSuspendProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, startStatusReadyConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartProducerSuspendConsumerSuspend() {
		StreamConsumer<Integer> startStatusSuspendConsumer = new TestConsumerOneByOne(eventloop) {{
			suspend();
		}};
		StreamProducer<Integer> startStatusSuspendProducer = new TestProducerOfIterator(eventloop, it) {{
			onConsumerSuspended();
		}};
		startStatusSuspendProducer.streamTo(startStatusSuspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(SUSPENDED, startStatusSuspendProducer.getProducerStatus());
		assertEquals(SUSPENDED, startStatusSuspendConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartProducerSuspendConsumerEndOfStream() {
		StreamConsumer<Integer> startStatusEndConsumer = new TestConsumerOneByOne(eventloop) {{
			onProducerEndOfStream();
		}};
		StreamProducer<Integer> startStatusSuspendProducer = new TestProducerOfIterator(eventloop, it) {{
			onConsumerSuspended();
		}};
		startStatusSuspendProducer.streamTo(startStatusEndConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, startStatusSuspendProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, startStatusEndConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartProducerSuspendConsumerClosedWithError() {
		StreamConsumer<Integer> startStatusClosedWithErrorConsumer = new TestConsumerOneByOne(eventloop) {{
			closeWithError(new Exception());
		}};
		StreamProducer<Integer> startStatusSuspendProducer = new TestProducerOfIterator(eventloop, it) {{
			onConsumerSuspended();
		}};
		startStatusSuspendProducer.streamTo(startStatusClosedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, startStatusSuspendProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, startStatusClosedWithErrorConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	//------------------------------------------------------------------------------------------------------------------

	@Test
	public void testStartProducerEndOfStreamConsumerReady() {
		StreamConsumer<Integer> startStatusReadyConsumer = new TestConsumerOneByOne(eventloop);
		StreamProducer<Integer> startStatusEndProducer = new TestProducerOfIterator(eventloop, it) {{
			sendEndOfStream();
		}};
		startStatusEndProducer.streamTo(startStatusReadyConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(END_OF_STREAM, startStatusEndProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, startStatusReadyConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartProducerEndOfStreamConsumerSuspend() {
		StreamConsumer<Integer> startStatusSuspendConsumer = new TestConsumerOneByOne(eventloop) {{
			suspend();
		}};
		StreamProducer<Integer> startStatusEndProducer = new TestProducerOfIterator(eventloop, it) {{
			sendEndOfStream();
		}};
		startStatusEndProducer.streamTo(startStatusSuspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(END_OF_STREAM, startStatusEndProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, startStatusSuspendConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartProducerEndOfStreamConsumerEndOfStream() {
		StreamConsumer<Integer> startStatusEndConsumer = new TestConsumerOneByOne(eventloop) {{
			onProducerEndOfStream();
		}};
		StreamProducer<Integer> startStatusEndProducer = new TestProducerOfIterator(eventloop, it) {{
			sendEndOfStream();
		}};
		startStatusEndProducer.streamTo(startStatusEndConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(END_OF_STREAM, startStatusEndProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, startStatusEndConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartProducerEndOfStreamConsumerClosedWithError() {
		StreamConsumer<Integer> startStatusClosedWithErrorConsumer = new TestConsumerOneByOne(eventloop) {{
			closeWithError(new Exception());
		}};
		StreamProducer<Integer> startStatusEndProducer = new TestProducerOfIterator(eventloop, it) {{
			sendEndOfStream();
		}};
		startStatusEndProducer.streamTo(startStatusClosedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(END_OF_STREAM, startStatusEndProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, startStatusClosedWithErrorConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	//------------------------------------------------------------------------------------------------------------------

	@Test
	public void testStartCloseWithErrorConsumerReady() {
		StreamConsumer<Integer> startStatusReadyConsumer = new TestConsumerOneByOne(eventloop);
		StreamProducer<Integer> startStatusClosedWithErrorProducer = new TestProducerOfIterator(eventloop, it) {{
			closeWithError(new Exception());
		}};
		startStatusClosedWithErrorProducer.streamTo(startStatusReadyConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, startStatusClosedWithErrorProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, startStatusReadyConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartCloseWithErrorConsumerSuspend() {
		StreamConsumer<Integer> startStatusSuspendConsumer = new TestConsumerOneByOne(eventloop) {{
			suspend();
		}};
		StreamProducer<Integer> startStatusClosedWithErrorProducer = new TestProducerOfIterator(eventloop, it) {{
			closeWithError(new Exception());
		}};
		startStatusClosedWithErrorProducer.streamTo(startStatusSuspendConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, startStatusClosedWithErrorProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, startStatusSuspendConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartCloseWithErrorConsumerEndOfStream() {
		StreamConsumer<Integer> startStatusEndConsumer = new TestConsumerOneByOne(eventloop) {{
			onProducerEndOfStream();
		}};
		StreamProducer<Integer> startStatusClosedWithErrorProducer = new TestProducerOfIterator(eventloop, it) {{
			closeWithError(new Exception());
		}};
		startStatusClosedWithErrorProducer.streamTo(startStatusEndConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, startStatusClosedWithErrorProducer.getProducerStatus());
		assertEquals(END_OF_STREAM, startStatusEndConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testStartCloseWithErrorConsumerClosedWithError() {
		StreamConsumer<Integer> startStatusClosedWithErrorConsumer = new TestConsumerOneByOne(eventloop) {{
			closeWithError(new Exception());
		}};
		StreamProducer<Integer> startStatusClosedWithErrorProducer = new TestProducerOfIterator(eventloop, it) {{
			closeWithError(new Exception());
		}};
		startStatusClosedWithErrorProducer.streamTo(startStatusClosedWithErrorConsumer);
		eventloop.run();

		assertEquals(Collections.emptyList(), list);
		assertEquals(CLOSED_WITH_ERROR, startStatusClosedWithErrorProducer.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, startStatusClosedWithErrorConsumer.getConsumerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	//------------------------------------------------------------------------------------------------------------------

	private class TestConsumerOneByOne extends AbstractStreamConsumer<Integer> {

		protected TestConsumerOneByOne(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {

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

	}
}
