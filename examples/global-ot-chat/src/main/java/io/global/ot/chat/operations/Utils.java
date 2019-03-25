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

import io.global.chat.chatroom.messages.Message;
import io.global.chat.chatroom.messages.MessageOperation;

import java.util.function.Function;

public final class Utils {
	public static final int CONTENT_MAX_LENGTH = 10;
	public static final Function<MessageOperation, String> DIFF_TO_STRING = op -> {
		Message message = op.getMessage();
		String author = message.getAuthor();
		String allContent = message.getContent();
		String content = allContent.length() > CONTENT_MAX_LENGTH ?
				(allContent.substring(0, CONTENT_MAX_LENGTH) + "...") :
				allContent;
		return (op.isTombstone() ? "-" : "+") + '[' + author + ':' + content + ']';
	};

	private Utils() {
		throw new AssertionError();
	}
}
