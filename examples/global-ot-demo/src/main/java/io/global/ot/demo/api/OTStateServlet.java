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
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.WithMiddleware;
import io.datakernel.ot.OTStateManager;
import io.datakernel.util.Tuple4;
import io.global.ot.api.CommitId;
import io.global.ot.demo.operations.Operation;
import io.global.ot.demo.operations.OperationState;
import io.global.ot.graph.NodesWalker;

import java.util.List;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.ot.demo.util.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class OTStateServlet implements WithMiddleware {
	private final OTStateManager<CommitId, Operation> manager;
	private final NodesWalker<CommitId, Operation> walker;
	private final MiddlewareServlet servlet;

	private OTStateServlet(OTStateManager<CommitId, Operation> manager, NodesWalker<CommitId, Operation> walker) {
		this.manager = manager;
		this.walker = walker;
		this.servlet = getServlet();
	}

	public static OTStateServlet create(OTStateManager<CommitId, Operation> stateManager, NodesWalker<CommitId, Operation> walker) {
		return new OTStateServlet(stateManager, walker);
	}

	private MiddlewareServlet getServlet() {
		return MiddlewareServlet.create()
				.with(GET, "/info", info())
				.with(POST, "/add", add())
				.with(GET, "/sync", sync());
	}

	// region servlets
	private AsyncServlet sync() {
		return request -> manager.sync()
				.thenApply($ -> okText());
	}

	private AsyncServlet add() {
		return request -> request.getBody()
				.thenCompose(body -> {
					try {
						Operation operation = fromJson(OPERATION_CODEC, body.getString(UTF_8));
						manager.add(operation);
						return Promise.of(okText());
					} catch (ParseException e) {
						return Promise.<HttpResponse>ofException(e);
					} finally {
						body.recycle();
					}
				});
	}

	private AsyncServlet info() {
		return request -> walker.walk(manager.getRevision())
				.thenApply($ -> {
					Tuple4<CommitId, Integer, List<Operation>, String> infoTuple = new Tuple4<>(
							manager.getRevision(),
							((OperationState) manager.getState()).getCounter(),
							manager.getWorkingDiffs(),
							walker.toGraphViz()
					);
					return okJson()
							.withBody(toJson(INFO_CODEC, infoTuple).getBytes(UTF_8));
				});
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
	// endregion
}
