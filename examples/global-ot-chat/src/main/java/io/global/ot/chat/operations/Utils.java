/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.global.ot.chat.operations;

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpCookie;
import io.datakernel.http.HttpRequest;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.global.ot.api.CommitId;
import io.global.ot.common.ManagerProvider;

import java.util.UUID;
import java.util.function.Function;

import static io.global.common.CryptoUtils.toHexString;
import static java.util.Collections.singletonList;

public final class Utils {
	public static final String SESSION_ID = "SESSION_ID";
	public static final Function<CommitId, String> ID_TO_STRING = commitId -> toHexString(commitId.toBytes()).substring(0, 7);
	public static final Function<ChatOperation, String> DIFF_TO_STRING = op -> (op.isTombstone() ? "-" : "+") +
			'[' + op.getAuthor() + ':' + op.getContent() + ']';

	private Utils() {
		throw new AssertionError();
	}

	public static OTSystem<ChatOperation> createOTSystem() {
		return OTSystemImpl.<ChatOperation>create()
				.withEmptyPredicate(ChatOperation.class, ChatOperation::isEmpty)
				.withInvertFunction(ChatOperation.class, op -> singletonList(op.invert()))
				.withSquashFunction(ChatOperation.class, ChatOperation.class, (op1, op2) -> {
					if (op1.isEmpty()) {
						return op2;
					}
					if (op2.isEmpty()) {
						return op1;
					}
					if (op1.isInversionFor(op2)) {
						return ChatOperation.EMPTY;
					}
					return null;
				})
				.withTransformFunction(ChatOperation.class, ChatOperation.class, (left, right) -> TransformResult.of(right, left));
	}

	public static Promise<OTStateManager<CommitId, ChatOperation>> getManager(ManagerProvider<ChatOperation> managerProvider, HttpRequest request) {
		try {
			String id = request.getCookie(SESSION_ID);
			return managerProvider.get(id);
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	}

	public static AsyncServlet ensureSessionID(AsyncServlet servlet) {
		return request -> {
			try {
				String sessionId = request.getCookieOrNull(SESSION_ID);
				if (sessionId == null) {
					String newSessionId = UUID.randomUUID().toString();
					request.addCookie(HttpCookie.of(SESSION_ID, newSessionId));
					return servlet.serve(request)
							.thenApply(response -> response.withCookie(HttpCookie.of(SESSION_ID, newSessionId)));
				} else {
					return servlet.serve(request);
				}
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};
	}
}
