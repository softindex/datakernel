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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.config.Config;
import io.datakernel.crdt.CrdtData;
import io.datakernel.crdt.local.RuntimeCrdtClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.util.guice.OptionalDependency;

import java.util.concurrent.ExecutorService;

import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class CrdtHttpModule<K extends Comparable<K>, S> extends AbstractModule {

	@Provides
	@Singleton
	AsyncHttpServer provideServer(Eventloop eventloop, AsyncServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("crdt.http")));
	}

	@Provides
	@Singleton
	StaticLoader provideLoader(ExecutorService executor) {
		return StaticLoaders.ofClassPath(executor);
	}

	@Provides
	@Singleton
	AsyncServlet provideServlet(
			CrdtDescriptor<K, S> descriptor,
			RuntimeCrdtClient<K, S> client,
			OptionalDependency<BackupService<K, S>> backupService
	) {
		StructuredCodec<K> keyCodec = descriptor.getKeyCodec();
		StructuredCodec<S> stateCodec = descriptor.getStateCodec();

		StructuredCodec<CrdtData<K, S>> codec = tuple(CrdtData::new,
				CrdtData::getKey, descriptor.getKeyCodec(),
				CrdtData::getState, descriptor.getStateCodec());
		MiddlewareServlet servlet = MiddlewareServlet.create()
				.with(HttpMethod.POST, "/", request -> request.getBody().thenCompose(body -> {
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
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(HttpMethod.PUT, "/", request -> request.getBody().thenCompose(body -> {
					try {
						client.put(JsonUtils.fromJson(codec, body.getString(UTF_8)));
						return Promise.of(HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}))
				.with(HttpMethod.DELETE, "/", request -> request.getBody().thenCompose(body -> {
					try {
						K key = JsonUtils.fromJson(keyCodec, body.getString(UTF_8));
						if (client.remove(key)) {
							return Promise.of(HttpResponse.ok200());
						}
						return Promise.of(HttpResponse.ofCode(404)
								.withBody(("Key '" + key + "' not found").getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				}));
		//		jmxServlet.ifPresent(s -> servlet.with(HttpMethod.GET, "/", s));
		backupService.ifPresent(backup -> servlet
				.with(HttpMethod.POST, "/backup", request -> {
					if (backup.backupInProgress()) {
						return Promise.of(HttpResponse.ofCode(403)
								.withBody("Backup is already in progress".getBytes(UTF_8)));
					}
					backup.backup();
					return Promise.of(HttpResponse.ofCode(202));
				})
				.with(HttpMethod.POST, "/awaitBackup", request ->
						backup.backupInProgress() ?
								backup.backup().thenApply($ -> HttpResponse.ofCode(204)
										.withBody("Finished already running backup".getBytes(UTF_8))) :
								backup.backup().thenApply($ -> HttpResponse.ok200())));
		return servlet;
	}
}
