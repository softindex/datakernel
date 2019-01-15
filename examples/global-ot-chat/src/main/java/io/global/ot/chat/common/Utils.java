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

package io.global.ot.chat.common;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.util.Tuple2;
import io.global.ot.api.CommitId;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.global.ot.util.HttpDataFormats.urlDecodeCommitId;
import static io.global.ot.util.HttpDataFormats.urlEncodeCommitId;

public class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final StructuredCodec<CommitId> URL_ENCODED_COMMIT_ID = StructuredCodec.of(
			in -> urlDecodeCommitId(in.readString()),
			(out, item) -> out.writeString(urlEncodeCommitId(item))
	);

	public static <D> StructuredCodec<Tuple2<CommitId, List<D>>> getTupleCodec(StructuredCodec<D> diffCodec) {
		return tuple(Tuple2::new,
				Tuple2::getValue1, URL_ENCODED_COMMIT_ID,
				Tuple2::getValue2, StructuredCodecs.ofList(diffCodec));
	}

}
