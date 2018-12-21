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
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.exceptions.OTTransformException;
import io.global.ot.api.CommitId;
import io.global.ot.demo.operations.AddOperation;
import io.global.ot.demo.operations.OperationState;
import io.global.ot.demo.state.StateManagerInfo;
import io.global.ot.demo.state.StateManagerProvider;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpMethod.*;
import static io.global.ot.demo.util.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;

public class StateAPI {
	private static final StateManagerInfo INFO = new StateManagerInfo();
	private final StateManagerProvider provider;

	public StateAPI(StateManagerProvider provider) {
		this.provider = provider;
	}

	public MiddlewareServlet getServlet() {
		return MiddlewareServlet.create()
				.with(PUT, "/add", add())
				.with(POST, "/commit", commit())
				.with(GET, "/info", info())
				.with(GET, "/state", state())
				.with(GET, "/checkout", checkout())
				.with(POST, "/push", push())
				.with(GET, "/graph", graph())
				.with(POST, "/merge", merge())
				.with(GET, "/pull", pull())
				.with(GET, "/fetch", fetch())
				.with(POST, "/reset", reset())
				.with(POST, "/rebase", rebase());
	}

	// region servlets
	private AsyncServlet rebase() {
		return request -> provider.get(request)
				.thenApply(manager -> {
					try {
						manager.rebase();
					} catch (OTTransformException e) {
						throw new UncheckedException(e);
					}
					return okText();
				});
	}

	private AsyncServlet reset() {
		return request -> provider.get(request)
				.whenResult(OTStateManager::reset)
				.thenApply($ -> okText());
	}

	private AsyncServlet fetch() {
		return request -> {
			try {
				String stringCommitId = request.getQueryParameter("commitId");
				CommitId commitId = stringCommitId.isEmpty() ? null : JsonUtils.fromJson(COMMIT_ID_HASH, stringCommitId);
				return provider.get(request)
						.thenCompose(manager -> commitId != null ?
								manager.fetch(commitId) :
								manager.fetch()
										.thenCompose(rev -> provider.getWalker(manager).walkFull()
												.thenApply($ -> rev))
						)
						.thenApply(fetchedRev -> okText()
								.withBody(wrapUtf8(String.valueOf(fetchedRev))));
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};
	}

	private AsyncServlet pull() {
		return request -> provider.get(request)
				.thenCompose(manager -> manager.pull()
						.thenCompose(pulledId -> provider.getWalker(manager).walk(singleton(pulledId))))
				.thenApply($ -> okText());
	}

	private AsyncServlet merge() {
		return request -> provider.get(request)
				.thenCompose(manager -> manager.getAlgorithms().mergeHeadsAndPush()
						.thenCompose(mergeId -> provider.getWalker(manager).walk(singleton(mergeId))))
				.thenApply($ -> okText());
	}

	private AsyncServlet graph() {
		return request -> provider.get(request)
				.thenApply(manager -> okText()
						.withBody(provider.getWalker(manager).toGraphViz().getBytes(UTF_8)));
	}

	private AsyncServlet push() {
		return request -> provider.get(request)
				.thenCompose(manager -> manager.push()
						.thenCompose($ -> provider.getWalker(manager).walk()))
				.thenApply($ -> okText());
	}

	private AsyncServlet checkout() {
		return request -> {
			try {
				CommitId commitId = JsonUtils.fromJson(COMMIT_ID_HASH, request.getQueryParameter("commitId"));
				return provider.get(request)
						.thenCompose(manager -> manager.checkout(commitId))
						.thenApply($ -> okText());
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};
	}

	private AsyncServlet state() {
		return request -> provider.get(request)
				.thenApply(manager -> ((OperationState) manager.getState()).getCounter())
				.thenApply(counter -> okJson()
						.withBody(toJson(INT_CODEC, counter).getBytes(UTF_8)));
	}

	private AsyncServlet commit() {
		return request -> provider.get(request)
				.thenCompose(manager -> manager.commit()
						.thenCompose($ -> provider.getWalker(manager).walk()))
				.thenApply($ -> okText());
	}

	private AsyncServlet add() {
		return request -> {
			try {
				Integer value = request.parseQueryParameter("value", Integer::parseInt);
				return provider.get(request)
						.thenApply(manager -> {
							manager.add(AddOperation.add(value));
							return okJson()
									.withBody(toJson(LIST_DIFFS_CODEC, manager.getWorkingDiffs()).getBytes(UTF_8));
						});
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};
	}

	private AsyncServlet info() {
		return request -> provider.get(request)
				.thenApply(manager -> {
							String redirectUrl = provider.isNew() ? ("../?id=" + provider.getId(manager)) : "";
							INFO.setRevision(manager.getRevision());
							INFO.setFetchedRevision(manager.getFetchedRevisionOrNull());
							INFO.setWorkingDiffs(manager.getWorkingDiffs());
							INFO.setState(manager.getState());
							return redirectUrl.isEmpty() ?
									okJson().withBody(toJson(INFO_CODEC, INFO).getBytes(UTF_8)) :
									HttpResponse.redirect302(redirectUrl);
						}
				);
	}
	// endregion
}
