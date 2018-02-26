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

package io.datakernel.rpc.protocol;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.net.UnknownHostException;

import static java.lang.ClassLoader.getSystemClassLoader;

public class RpcMessageSerializeTest {

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

	private static <T> T doTest(Class<T> type, T testData1) {
		BufferSerializer<T> serializer = SerializerBuilder.create(getSystemClassLoader())
				.withSubclasses(RpcMessage.MESSAGE_TYPES, TestRpcMessageData.class, TestRpcMessageData2.class)
				.build(type);
		return doTest(testData1, serializer, serializer);
	}

	private static <T> T doTest(T testData1, BufferSerializer<T> serializer, BufferSerializer<T> deserializer) {
		byte[] array = new byte[1000];
		ByteBuf buf = ByteBuf.wrapForWriting(array);
		serializer.serialize(buf, testData1);
		return deserializer.deserialize(buf);
	}

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testRpcMessage() throws UnknownHostException {
		TestRpcMessageData messageData1 = new TestRpcMessageData("TestMessageData");
		RpcMessage message1 = RpcMessage.of(1, messageData1);

		RpcMessage message2 = doTest(RpcMessage.class, message1);
		Assert.assertEquals(message1.getCookie(), message2.getCookie());
		Assert.assertTrue(message2.getData() instanceof TestRpcMessageData);
		TestRpcMessageData messageData2 = (TestRpcMessageData) message2.getData();
		Assert.assertEquals(messageData1.getS(), messageData2.getS());
	}

}
