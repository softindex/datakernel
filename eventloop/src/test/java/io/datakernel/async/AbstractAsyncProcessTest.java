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

package io.datakernel.async;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.exception.ParseException;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.WithSerialToSerial;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static io.datakernel.util.Recyclable.deepRecycle;
import static io.datakernel.util.Recyclable.tryRecycle;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AbstractAsyncProcessTest {
	@Rule
	public final ByteBufRule byteBufRule = new ByteBufRule();

	private final int size = 10;
	private final List<ByteBuf> actualData = new ArrayList<>();
	private final ParseException error = new ParseException("Test Error");

	private Eventloop eventloop;

	private PassThroughProcess[] processes = new PassThroughProcess[size];
	private MaterializedStage<Void> acknowledgement;
	private List<ByteBuf> expectedData = new ArrayList<>();
	private boolean consumedAll = false;


	@Before
	public void setUp() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError()).withCurrentThread();
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
			processes[i].streamTo(processes[i + 1]);
		}

		acknowledgement = SerialSupplier.ofIterable(expectedData).streamTo(processes[0]);
	}

	@Test
	public void testAckPropagation() {
		processes[size - 1].streamTo(SerialConsumer.of(value -> {
			actualData.add(value);
			if (expectedData.size() == actualData.size()) {
				deepRecycle(actualData);
				consumedAll = true;
			}
			return Stage.complete();
		}));

		acknowledgement.whenComplete(assertComplete($ -> assertTrue(consumedAll)));

		eventloop.run();
	}

	@Test
	public void testAckPropagationWithFailure() {
		processes[size - 1].streamTo(SerialConsumer.of(value -> {
			tryRecycle(value);
			return Stage.ofException(error);
		}));

		acknowledgement.whenComplete(assertFailure(e -> assertSame(error, e)));

		eventloop.run();
	}

	// region stub
	static class PassThroughProcess extends AbstractAsyncProcess implements WithSerialToSerial<PassThroughProcess, ByteBuf, ByteBuf> {
		SerialSupplier<ByteBuf> input;
		SerialConsumer<ByteBuf> output;

		@Override
		public void setOutput(SerialConsumer<ByteBuf> output) {
			this.output = output;
		}

		@Override
		public MaterializedStage<Void> setInput(SerialSupplier<ByteBuf> input) {
			this.input = input;
			return getResult();
		}

		@Override
		protected void doProcess() {
			input.get()
					.whenComplete((data, e) -> {
						if (data == null) {
							output.accept(null)
									.whenException(this::closeWithError)
									.thenRun(this::completeProcess);
						} else {
							output.accept(data)
									.whenException(this::closeWithError)
									.thenRun(this::doProcess);
						}
					});
		}

		@Override
		protected void doCloseWithError(Throwable e) {
			output.closeWithError(e);
			input.closeWithError(e);
		}
	}
	// endregion
}
