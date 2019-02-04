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
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTRepository;
import io.datakernel.util.Tuple2;
import io.global.ot.api.CommitId;
import io.global.ot.demo.operations.Operation;
import io.global.ot.demo.operations.OperationState;
import io.global.ot.demo.util.StateManagerProvider;
import io.global.ot.graph.NodesWalker;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.ot.demo.util.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class OTStateServlet implements WithMiddleware {
	private final StateManagerProvider provider;
	private final OTAlgorithms<CommitId, Operation> algorithms;
	private final MiddlewareServlet servlet;

	private OTStateServlet(StateManagerProvider provider, OTAlgorithms<CommitId, Operation> algorithms) {
		this.provider = provider;
		this.algorithms = algorithms;
		this.servlet = getServlet();
	}

	public static OTStateServlet create(OTRepository<CommitId, Operation> repository, OTAlgorithms<CommitId, Operation> algorithms) {
		return new OTStateServlet(new StateManagerProvider(algorithms.getOtSystem(), algorithms.getOtNode(), repository), algorithms);
	}

	private MiddlewareServlet getServlet() {
		return MiddlewareServlet.create()
				.with(GET, "/info", info())
				.with(GET, "/state", state())
				.with(GET, "/checkout", checkout())
				.with(POST, "/push", push())
				.with(GET, "/graph", graph())
				.with(POST, "/merge", merge())
				.with(GET, "/pull", pull());
	}

	// region servlets
	private AsyncServlet pull() {
		return request -> provider.get(request)
				.thenCompose(manager -> manager.pull()
						.thenCompose(pulledId -> provider.getWalker(manager).walk(manager.getLocalRevision())))
				.thenApply($ -> okText());
	}

	private AsyncServlet merge() {
		return request -> provider.get(request)
				.thenCompose(manager -> algorithms.merge()
						.thenCompose(mergeId -> provider.getWalker(manager).walk(manager.getLocalRevision())))
				.thenApply($ -> okText());
	}

	private AsyncServlet graph() {
		return request -> provider.get(request)
				.thenCompose(manager -> {
					NodesWalker<CommitId, Operation> walker = provider.getWalker(manager);
					return walker.walk(manager.getLocalRevision())
							.thenApply($ -> okText()
									.withBody(walker.toGraphViz().getBytes(UTF_8)));
				});
	}

	private AsyncServlet push() {
		return request -> request.getBody()
				.thenCompose(body -> {
					try {
						List<Operation> operations = fromJson(LIST_DIFFS_CODEC, body.getString(UTF_8));
						return provider.get(request)
								.thenCompose(manager -> {
									manager.addAll(operations);
									return manager.commit()
											.thenCompose($ -> manager.push())
											.thenCompose($ -> provider.getWalker(manager).walk(manager.getLocalRevision()));
								})
								.thenApply($ -> okText());
					} catch (ParseException e) {
						return Promise.ofException(e);
					} finally {
						body.recycle();
					}
				});
	}

	private AsyncServlet checkout() {
		return request -> provider.get(request)
				.thenCompose(manager -> manager.checkout()
						.thenCompose($ -> provider.getWalker(manager).walk(manager.getLocalRevision())))
				.thenApply($ -> okText());
	}

	private AsyncServlet state() {
		return request -> provider.get(request)
				.thenApply(manager -> ((OperationState) manager.getState()).getCounter())
				.thenApply(counter -> okJson()
						.withBody(toJson(INT_CODEC, counter).getBytes(UTF_8)));
	}

	private AsyncServlet info() {
		return request -> provider.get(request)
				.thenApply(manager -> {
							String redirectUrl = provider.isNew() ? ("../?id=" + provider.getId(manager)) : "";
					Tuple2<CommitId, Integer> infoTuple = new Tuple2<>(
							manager.getLocalRevision(),
							((OperationState) manager.getState()).getCounter()
					);
							return redirectUrl.isEmpty() ?
									okJson().withBody(toJson(INFO_CODEC, infoTuple).getBytes(UTF_8)) :
									HttpResponse.redirect302(redirectUrl);
						}
				);
	}

	@Override
	public MiddlewareServlet getMiddlewareServlet() {
		return servlet;
	}
	// endregion
}
