/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.csp.async;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.*;
import io.datakernel.csp.dsl.WithChannelTransformer;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.datakernel.common.Recyclable.deepRecycle;
import static io.datakernel.common.Recyclable.tryRecycle;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public final class AbstractCommunicatingProcessTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final int size = 10;
	private final List<ByteBuf> actualData = new ArrayList<>();
	private final ParseException error = new ParseException(AbstractCommunicatingProcessTest.class, "Test Error");

	private PassThroughProcess[] processes = new PassThroughProcess[size];
	private Promise<Void> acknowledgement;
	private List<ByteBuf> expectedData = new ArrayList<>();
	private boolean consumedAll = false;

	@Before
	public void setUp() {
		Random random = new Random();
		for (int i = 0; i < 5; i++) {
			byte[] bytes = new byte[1000];
			random.nextBytes(bytes);
			ByteBuf buf = ByteBufPool.allocate(bytes.length);
			buf.put(bytes);
			expectedData.add(buf);
		}
		for (int i = 0; i < size; i++) {
			processes[i] = new PassThroughProcess();
		}

		for (int i = 0; i < size - 1; i++) {
			processes[i].getOutput().bindTo(processes[i + 1].getInput());
		}

		acknowledgement = ChannelSupplier.ofIterable(expectedData)
				.bindTo(processes[0].getInput());
	}

	@Test
	public void testAckPropagation() {
		processes[size - 1].getOutput()
				.set(ChannelConsumer.of(value -> {
					actualData.add(value);
					if (expectedData.size() == actualData.size()) {
						deepRecycle(actualData);
						consumedAll = true;
					}
					return Promise.complete();
				}));

		await(acknowledgement);
		assertTrue(consumedAll);
	}

	@Test
	public void testAckPropagationWithFailure() {
		processes[size - 1].getOutput()
				.set(ChannelConsumer.of(value -> {
					tryRecycle(value);
					return Promise.ofException(error);
				}));

		assertSame(error, awaitException(acknowledgement));
	}

	// region stub
	static class PassThroughProcess extends AbstractCommunicatingProcess implements WithChannelTransformer<PassThroughProcess, ByteBuf, ByteBuf> {
		ChannelSupplier<ByteBuf> input;
		ChannelConsumer<ByteBuf> output;

		@Override
		public ChannelInput<ByteBuf> getInput() {
			return input -> {
				this.input = input;
				if (this.input != null && this.output != null) startProcess();
				return getProcessCompletion();
			};
		}

		@Override
		public ChannelOutput<ByteBuf> getOutput() {
			return output -> {
				this.output = output;
				if (this.input != null && this.output != null) startProcess();
			};
		}

		@Override
		protected void doProcess() {
			input.get()
					.whenComplete((data, e) -> {
						if (data == null) {
							output.accept(null)
									.whenException(this::close)
									.whenResult($ -> completeProcess());
						} else {
							output.accept(data)
									.whenException(this::close)
									.whenResult(this::doProcess);
						}
					});
		}

		@Override
		protected void doClose(Throwable e) {
			output.close(e);
			input.close(e);
		}
	}
	// endregion
}
