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

package io.global.ot.editor.operations;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;

import static io.global.ot.editor.operations.DeleteOperation.DELETE_CODEC;
import static io.global.ot.editor.operations.InsertOperation.INSERT_CODEC;

public final class Utils {
	public static final StructuredCodec<EditorOperation> OPERATION_CODEC = StructuredCodec.ofObject(
			in -> {
				in.readKey("type");
				String type = in.readString();
				in.readKey("value");
				switch (type) {
					case "Insert":
						return INSERT_CODEC.decode(in);
					case "Delete":
						return DELETE_CODEC.decode(in);
					default:
						throw new ParseException("Either Insert or Delete is expected");
				}
			}, (out, item) -> {
				out.writeKey("type");
				if (item instanceof InsertOperation) {
					out.writeString("Insert");
					out.writeKey("value", INSERT_CODEC, (InsertOperation) item);
				} else if (item instanceof DeleteOperation) {
					out.writeString("Delete");
					out.writeKey("value", DELETE_CODEC, (DeleteOperation) item);
				} else {
					throw new IllegalArgumentException("Item should be either InsertOperation or DeleteOperation");
				}
			}
	);

	private Utils() {
		throw new AssertionError();
	}

	public static String limit(String initial, int limit) {
		int length = initial.length();
		return length > limit ?
				(initial.substring(0, limit) + "...") :
				initial;
	}
}
