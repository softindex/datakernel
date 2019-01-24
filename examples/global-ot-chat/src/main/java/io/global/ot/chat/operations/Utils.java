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
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.global.ot.api.CommitId;

import java.util.UUID;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static javax.xml.bind.DatatypeConverter.printHexBinary;

public final class Utils {

	public static final String SESSION_ID = "SESSION_ID";

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

	public static Function<CommitId, String> getCommitIdToString() {
		return commitId -> printHexBinary(commitId.toBytes()).substring(0, 7).toLowerCase();
	}

	public static Function<ChatOperation, String> getChatOperationToString() {
		return op -> (op.isTombstone() ? "-" : "+") + '[' + op.getAuthor() + ':' + op.getContent() + ']';
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
