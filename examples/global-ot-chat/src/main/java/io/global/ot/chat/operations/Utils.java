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

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;

import java.util.function.Function;

import static java.util.Collections.singletonList;

public final class Utils {
	public static final int CONTENT_MAX_LENGTH = 10;
	public static final Function<ChatOperation, String> DIFF_TO_STRING = op -> {
		String author = op.getAuthor();
		String allContent = op.getContent();
		String content = allContent.length() > CONTENT_MAX_LENGTH ?
				(allContent.substring(0, CONTENT_MAX_LENGTH) + "...") :
				allContent;
		return (op.isTombstone() ? "-" : "+") + '[' + author + ':' + content + ']';
	};

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

}
