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

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.global.ot.api.CommitId;

import java.util.function.Function;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.common.CryptoUtils.toHexString;

public final class Utils {
	public static final Function<CommitId, String> ID_TO_STRING = commitId -> toHexString(commitId.toBytes()).substring(0, 7);
	public static final StructuredCodec<EditorOperation> OPERATION_CODEC = CodecSubtype.<EditorOperation>create()
			.with(InsertOperation.class, "Insert", object(InsertOperation::new,
					"pos", InsertOperation::getPosition, INT_CODEC,
					"content", InsertOperation::getContent, STRING_CODEC))
			.with(DeleteOperation.class, "Delete", object(DeleteOperation::new,
					"pos", DeleteOperation::getPosition, INT_CODEC,
					"content", DeleteOperation::getContent, STRING_CODEC));

	private Utils() {
		throw new AssertionError();
	}
}
