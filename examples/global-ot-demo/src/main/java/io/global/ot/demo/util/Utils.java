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

package io.global.ot.demo.util;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpResponse;
import io.datakernel.util.Tuple2;
import io.global.ot.api.CommitId;
import io.global.ot.demo.operations.AddOperation;
import io.global.ot.demo.operations.Operation;

import java.util.List;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.http.MediaTypes.PLAIN_TEXT;
import static io.global.common.CryptoUtils.fromHexString;
import static io.global.common.CryptoUtils.toHexString;

@SuppressWarnings("WeakerAccess")
public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final Function<CommitId, String> ID_TO_STRING = commitId -> toHexString(commitId.toBytes()).substring(0, 7);
	public static final Function<Operation, String> OPERATION_TO_STRING = operation -> {
		int value = operation.getValue();
		return (value > 0 ? "+" : "-") + value;
	};

	public static final StructuredCodec<Operation> OPERATION_CODEC = StructuredCodec.of(
			in -> AddOperation.add(Integer.parseInt(in.readString())),
			(out, item) -> {
				int value = item.getValue();
				out.writeString((value > 0 ? "+" : "") + value);
			});

	public static final StructuredCodec<List<Operation>> LIST_DIFFS_CODEC = ofList(OPERATION_CODEC);

	public static final StructuredCodec<CommitId> COMMIT_ID_HASH = StructuredCodec.of(
			in -> CommitId.ofBytes(fromHexString(in.readString())),
			(out, item) -> out.writeString(toHexString(item.toBytes()))
	);

	public static final StructuredCodec<Tuple2<CommitId, Integer>> INFO_CODEC = tuple(Tuple2::new,
			Tuple2::getValue1, COMMIT_ID_HASH,
			Tuple2::getValue2, INT_CODEC);


	public static HttpResponse okText() {
		return HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(PLAIN_TEXT)));
	}

	public static HttpResponse okJson() {
		return HttpResponse.ok200().withHeader(CONTENT_TYPE, ofContentType(ContentType.of(JSON)));
	}
}
