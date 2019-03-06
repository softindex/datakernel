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

package io.global.ot.demo.api;

import io.datakernel.async.Promise;
import io.datakernel.exception.ConstantException;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.WithMiddleware;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTStateManager;
import io.datakernel.util.Tuple4;
import io.global.ot.api.CommitId;
import io.global.ot.common.ManagerProvider;
import io.global.ot.demo.operations.Operation;
import io.global.ot.demo.operations.OperationState;

import java.time.Duration;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.ot.demo.util.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class OTStateServlet implements WithMiddleware {
	private static final ConstantException MANAGER_NOT_INITIALIZED = new ConstantException(OTStateServlet.class, "Manager has not been initialized yet");
	private final ManagerProvider<Operation> provider;
	private final OTAlgorithms<CommitId, Operation> algorithms;
	private final MiddlewareServlet servlet;

	private OTStateServlet(ManagerProvider<Operation> provider, OTAlgorithms<CommitId, Operation> algorithms) {
		this.provider = provider;
		this.algorithms = algorithms;
		this.servlet = getServlet();
	}

	public static OTStateServlet create(OTAlgorithms<CommitId, Operation> algorithms, Duration syncInterval) {
		return new OTStateServlet(new ManagerProvider<>(algorithms, OperationState::new, syncInterval), algorithms);
	}

	private MiddlewareServlet getServlet() {
		return MiddlewareServlet.create()
				.with(GET, "/info", info())
				.with(POST, "/add", add())
				.with(GET, "/sync", sync());
	}

	// region servlets
	private AsyncServlet sync() {
		return request -> getManager(provider, request)
				.thenCompose(OTStateManager::sync)
				.thenApply($ -> okText());
	}

	private AsyncServlet add() {
		return request -> request.getBody()
				.thenCompose(body -> getManager(provider, request)
						.thenCompose(manager -> {
							if (manager != null) {
								try {
									Operation operation = fromJson(OPERATION_CODEC, body.getString(UTF_8));
									manager.add(operation);
									return Promise.of(okText());
								} catch (ParseException e) {
									return Promise.<HttpResponse>ofException(e);
								} finally {
									body.recycle();
								}
							} else {
								return Promise.ofException(MANAGER_NOT_INITIALIZED);
							}
						}));
	}

	private AsyncServlet info() {
		return request -> getManager(provider, request)
				.thenCompose(manager -> {
					if (manager != null) {
						return algorithms.getRepository()
								.getHeads()
								.thenCompose(heads -> algorithms.loadGraph(heads, ID_TO_STRING, DIFF_TO_STRING))
								.thenApply(graph -> {
									String status = manager.hasPendingCommits() || manager.hasWorkingDiffs() ? "Syncing" : "Synced";
									Tuple4<CommitId, Integer, String, String> infoTuple = new Tuple4<>(
											manager.getCommitId(),
											((OperationState) manager.getState()).getCounter(),
											status,
											graph.toGraphViz(manager.getCommitId())
									);
									return okJson().withBody(toJson(INFO_CODEC, infoTuple).getBytes(UTF_8));
								});
					} else {
						return Promise.of(HttpResponse.redirect302("../?id=" + getNextId(provider)));
					}
				});
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
	// endregion
}
