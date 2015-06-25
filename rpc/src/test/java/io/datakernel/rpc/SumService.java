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

package io.datakernel.rpc;

import com.google.common.primitives.Doubles;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;
import io.datakernel.rpc.protocol.RpcMessageSerializer;
import io.datakernel.rpc.server.RequestHandlers;
import io.datakernel.rpc.server.RequestHandlers.RequestHandler;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;

import java.util.concurrent.atomic.AtomicInteger;

public class SumService implements RequestHandler<SumService.Request> {

	public static final class Request extends RpcMessage.AbstractRpcMessage {
		private int a, b;

		@Serialize(order = 0)
		public int getA() {
			return a;
		}

		public void setA(int a) {
			this.a = a;
		}

		@Serialize(order = 1)
		public int getB() {
			return b;
		}

		public void setB(int b) {
			this.b = b;
		}

		@Override
		public String toString() {
			return "Request[a=" + a + ", b=" + b + "]";
		}
	}

	public static final class Response extends RpcMessage.AbstractRpcMessage {
		private final String result;

		public Response(@Deserialize("result") String result) {
			this.result = result;
		}

		@Serialize(order = 0)
		public String getResult() {
			return result;
		}
	}

	private final AtomicInteger totalRequests = new AtomicInteger(0);

	@Override
	public void run(Request request, ResultCallback<RpcMessage.RpcMessageData> callback) {
		totalRequests.incrementAndGet();
		String result = Integer.toString(request.a + request.b);
		callback.onResult(new Response(result));
	}

	public int totalRequests() {
		return totalRequests.get();
	}

	public static RequestHandlers createHandlers(SumService service) {
		return new RequestHandlers.Builder().put(Request.class, service).build();
	}

	public static RpcMessageSerializer serializer() {
		return RpcMessageSerializer.builder().addExtraRpcMessageType(Request.class, Response.class).build();
	}

	public static HashFunction<RpcMessageData> consistentHashFunction() {
		return new HashFunction<RpcMessage.RpcMessageData>() {
			@Override
			public int hashCode(RpcMessage.RpcMessageData item) {
				int a = ((Request) item).getA();
				return Doubles.hashCode((a + 1) * 1234567.8);
			}
		};
	}

	public static HashFunction<RpcMessage.RpcMessageData> simpleHashFunction() {
		return new HashFunction<RpcMessage.RpcMessageData>() {
			@Override
			public int hashCode(RpcMessage.RpcMessageData item) {
				return ((Request) item).getA();
			}
		};
	}
}
