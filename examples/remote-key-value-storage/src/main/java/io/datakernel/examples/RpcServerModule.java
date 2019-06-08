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
package io.datakernel.examples;

import io.datakernel.async.Promise;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.annotation.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.SerializerBuilder;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

// [START EXAMPLE]
public class RpcServerModule extends AbstractModule {
	private static final int RPC_SERVER_PORT = 5353;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create()
				.withFatalErrorHandler(rethrowOnAnyError());
	}

	@Provides
	KeyValueStore keyValueStore() {
		return new KeyValueStore();
	}

	@Provides
	RpcServer rpcServer(Eventloop eventloop, KeyValueStore store) {
		return RpcServer.create(eventloop)
				.withSerializerBuilder(SerializerBuilder.create(Thread.currentThread().getContextClassLoader()))
				.withMessageTypes(PutRequest.class, PutResponse.class, GetRequest.class, GetResponse.class)
				.withHandler(PutRequest.class, PutResponse.class, req -> Promise.of(new PutResponse(store.put(req.getKey(), req.getValue()))))
				.withHandler(GetRequest.class, GetResponse.class, req -> Promise.of(new GetResponse(store.get(req.getKey()))))
				.withListenPort(RPC_SERVER_PORT);
	}
}
// [END EXAMPLE]
