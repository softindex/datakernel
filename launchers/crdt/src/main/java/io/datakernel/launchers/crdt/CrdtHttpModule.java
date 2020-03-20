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

package io.datakernel.launchers.crdt;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.common.parse.ParseException;
import io.datakernel.config.Config;
import io.datakernel.crdt.CrdtData;
import io.datakernel.crdt.local.CrdtClientMap;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.http.loader.StaticLoader;
import io.datakernel.promise.Promise;

import java.util.concurrent.Executor;

import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class CrdtHttpModule<K extends Comparable<K>, S> extends AbstractModule {

	@Provides
	AsyncHttpServer server(Eventloop eventloop, AsyncServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("crdt.http")));
	}

	@Provides
	StaticLoader loader(Executor executor) {
		return StaticLoader.ofClassPath(executor, "/");
	}

	@Provides
	AsyncServlet servlet(
			CrdtDescriptor<K, S> descriptor,
			CrdtClientMap<K, S> client,
			@Optional BackupService<K, S> backupService
	) {
		StructuredCodec<K> keyCodec = descriptor.getKeyCodec();
		StructuredCodec<S> stateCodec = descriptor.getStateCodec();

		StructuredCodec<CrdtData<K, S>> codec = tuple(CrdtData::new,
				CrdtData::getKey, descriptor.getKeyCodec(),
				CrdtData::getState, descriptor.getStateCodec());
		RoutingServlet servlet = RoutingServlet.create()
				.map(POST, "/", loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							try {
								K key = JsonUtils.fromJson(keyCodec, body.getString(UTF_8));
								S state = client.get(key);
								if (state != null) {
									return Promise.of(HttpResponse.ok200()
											.withBody(JsonUtils.toJson(stateCodec, state).getBytes(UTF_8)));
								}
								return Promise.of(HttpResponse.ofCode(404)
										.withBody(("Key '" + key + "' not found").getBytes(UTF_8)));
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, e));
							}
						}))
				.map(PUT, "/", loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							try {
								client.put(JsonUtils.fromJson(codec, body.getString(UTF_8)));
								return Promise.of(HttpResponse.ok200());
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, e));
							}
						}))
				.map(DELETE, "/", loadBody()
						.serve(request -> {
							ByteBuf body = request.getBody();
							try {
								K key = JsonUtils.fromJson(keyCodec, body.getString(UTF_8));
								if (client.remove(key)) {
									return Promise.of(HttpResponse.ok200());
								}
								return Promise.of(HttpResponse.ofCode(404)
										.withBody(("Key '" + key + "' not found").getBytes(UTF_8)));
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, e));
							}
						}));
		if (backupService == null) {
			return servlet;
		}
		return servlet
				.map(POST, "/backup", request -> {
					if (backupService.backupInProgress()) {
						return Promise.of(HttpResponse.ofCode(403)
								.withBody("Backup is already in progress".getBytes(UTF_8)));
					}
					backupService.backup();
					return Promise.of(HttpResponse.ofCode(202));
				})
				.map(POST, "/awaitBackup", request ->
						backupService.backupInProgress() ?
								backupService.backup().map($ -> HttpResponse.ofCode(204)
										.withBody("Finished already running backup".getBytes(UTF_8))) :
								backupService.backup().map($ -> HttpResponse.ok200()));
	}
}
