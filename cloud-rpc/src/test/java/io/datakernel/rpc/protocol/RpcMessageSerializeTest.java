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

package io.datakernel.rpc.protocol;

import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.LoggingRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static java.lang.ClassLoader.getSystemClassLoader;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class RpcMessageSerializeTest {

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	public static class TestRpcMessageData {
		private final String s;

		public TestRpcMessageData(@Deserialize("s") String s) {
			this.s = s;
		}

		@Serialize(order = 0)
		public String getS() {
			return s;
		}

	}

	public static class TestRpcMessageData2 {
		private final int i;

		public TestRpcMessageData2(@Deserialize("i") int i) {
			this.i = i;
		}

		@Serialize(order = 0)
		public int getI() {
			return i;
		}

	}

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testRpcMessage() {
		TestRpcMessageData messageData1 = new TestRpcMessageData("TestMessageData");
		RpcMessage message1 = RpcMessage.of(1, messageData1);
		BinarySerializer<RpcMessage> serializer = SerializerBuilder.create(getSystemClassLoader())
				.withSubclasses(RpcMessage.MESSAGE_TYPES, TestRpcMessageData.class, TestRpcMessageData2.class)
				.build(RpcMessage.class);

		byte[] buf = new byte[1000];
		serializer.encode(buf, 0, message1);
		RpcMessage message2 = serializer.decode(buf, 0);
		assertEquals(message1.getCookie(), message2.getCookie());
		assertTrue(message2.getData() instanceof TestRpcMessageData);
		TestRpcMessageData messageData2 = (TestRpcMessageData) message2.getData();
		assertEquals(messageData1.getS(), messageData2.getS());
	}
}
