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

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.ot.OTStateManager;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.util.Tuple4;
import io.global.ot.api.CommitId;
import io.global.ot.common.ManagerProvider;
import io.global.ot.demo.operations.AddOperation;
import io.global.ot.demo.operations.Operation;

import java.util.function.Function;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.http.MediaTypes.PLAIN_TEXT;
import static io.global.common.CryptoUtils.fromHexString;
import static io.global.common.CryptoUtils.toHexString;
import static io.global.ot.demo.operations.AddOperation.add;
import static java.util.Collections.singletonList;

@SuppressWarnings("WeakerAccess")
public final class Utils {
	public static final Function<CommitId, String> ID_TO_STRING = commitId -> toHexString(commitId.toBytes()).substring(0, 7);
	public static final Function<Operation, String> DIFF_TO_STRING = operation -> {
		int value = operation.getValue();
		return (value > 0 ? "+" : "-") + value;
	};

	private Utils() {
		throw new AssertionError();
	}

	public static final StructuredCodec<Operation> OPERATION_CODEC = StructuredCodec.of(
			in -> AddOperation.add(Integer.parseInt(in.readString())),
			(out, item) -> {
				int value = item.getValue();
				out.writeString((value > 0 ? "+" : "") + value);
			});

	public static final StructuredCodec<CommitId> COMMIT_ID_HASH = StructuredCodec.of(
			in -> CommitId.ofBytes(fromHexString(in.readString())),
			(out, item) -> out.writeString(toHexString(item.toBytes()))
	);

	public static final StructuredCodec<Tuple4<CommitId, Integer, String, String>> INFO_CODEC = tuple(Tuple4::new,
			Tuple4::getValue1, COMMIT_ID_HASH,
			Tuple4::getValue2, INT_CODEC,
			Tuple4::getValue3, STRING_CODEC,
			Tuple4::getValue4, STRING_CODEC);

	public static HttpResponse okText() {
		return HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(PLAIN_TEXT)));
	}

	public static HttpResponse okJson() {
		return HttpResponse.ok200().withHeader(CONTENT_TYPE, ofContentType(ContentType.of(JSON)));
	}

	public static OTSystem<Operation> createOTSystem() {
		return OTSystemImpl.<Operation>create()
				.withTransformFunction(AddOperation.class, AddOperation.class, (left, right) -> TransformResult.of(right, left))
				.withEmptyPredicate(AddOperation.class, addOperation -> addOperation.getValue() == 0)
				.withInvertFunction(AddOperation.class, addOperation -> singletonList(add(-addOperation.getValue())))
				.withSquashFunction(AddOperation.class, AddOperation.class, (op1, op2) -> add(op1.getValue() + op2.getValue()));
	}

	public static Promise<OTStateManager<CommitId, Operation>> getManager(ManagerProvider<Operation> managerProvider, HttpRequest request) {
		String id = request.getQueryParameterOrNull("id");
		if (id == null || id.isEmpty()) {
			return Promise.of(null);
		} else {
			return managerProvider.get(id);
		}
	}

	public static String getNextId(ManagerProvider<Operation> managerProvider) {
		return String.valueOf(managerProvider.getIds()
				.stream()
				.map(Integer::valueOf)
				.sorted()
				.reduce(0, (acc, next) -> !next.equals(acc) ? acc : next + 1));
	}
}
