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

import com.google.common.reflect.TypeToken;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializationInputBuffer;
import io.datakernel.serializer.SerializationOutputBuffer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static org.junit.Assert.assertEquals;

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

	private static <T> T doTest(TypeToken<T> typeToken, T testData1) {
		BufferSerializer<T> serializer = SerializerBuilder
				.newDefaultInstance(ClassLoader.getSystemClassLoader())
				.setExtraSubclasses("extraRpcMessageData", TestRpcMessageData.class, TestRpcMessageData2.class)
				.create(typeToken.getRawType());
		return doTest(testData1, serializer, serializer);
	}

	private static <T> T doTest(T testData1, BufferSerializer<T> serializer, BufferSerializer<T> deserializer) {
		byte[] array = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(array);
		serializer.serialize(output, testData1);
		SerializationInputBuffer input = new SerializationInputBuffer(array, 0);
		return deserializer.deserialize(input);
	}

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	// TODO (vmykhalko): check whether this test is needed or not
//	@Test
//	public void testRpcMessageData() throws UnknownHostException {
//		TestRpcMessageData messageData1 = new TestRpcMessageData("TestMessageData");
//
//		Object messageData2 = doTest(TypeToken.of(Object.class), messageData1);
//		Assert.assertTrue(messageData2 instanceof TestRpcMessageData);
//
//		TestRpcMessageData testMessage2 = (TestRpcMessageData) messageData2;
//		Assert.assertEquals(messageData1.s, testMessage2.s);
//		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
//	}

	@Test
	public void testRpcMessage() throws UnknownHostException {
		TestRpcMessageData messageData1 = new TestRpcMessageData("TestMessageData");
		RpcMessage message1 = new RpcMessage(1, messageData1);

		RpcMessage message2 = doTest(TypeToken.of(RpcMessage.class), message1);
		Assert.assertEquals(message1.getCookie(), message2.getCookie());
		Assert.assertTrue(message2.getData() instanceof TestRpcMessageData);
		TestRpcMessageData messageData2 = (TestRpcMessageData) message2.getData();
		Assert.assertEquals(messageData1.getS(), messageData2.getS());
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

}
