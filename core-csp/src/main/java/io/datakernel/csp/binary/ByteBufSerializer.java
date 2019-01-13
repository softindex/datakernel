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

package io.datakernel.csp.binary;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.binary.BinaryUtils;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.util.ByteBufPoolAppendable;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface ByteBufSerializer<I, O> extends ByteBufsParser<I> {
	ByteBuf serialize(O item);

	static <I, O> ByteBufSerializer<I, O> ofBinaryCodec(StructuredCodec<I> responseCodec, StructuredCodec<O> requestCodec) {
		ByteBufsParser<I> parser = ByteBufsParser.ofDecoder(responseCodec);
		return new ByteBufSerializer<I, O>() {
			@Override
			public ByteBuf serialize(O item) {
				return BinaryUtils.encode(requestCodec, item);
			}

			@Nullable
			@Override
			public I tryParse(ByteBufQueue bufs) throws ParseException {
				return parser.tryParse(bufs);
			}
		};
	}

	static <T> ByteBufSerializer<T, T> ofBinaryCodec(StructuredCodec<T> codec) {
		return ofBinaryCodec(codec, codec);
	}

	static <I, O> ByteBufSerializer<I, O> ofJsonCodec(StructuredCodec<I> in, StructuredCodec<O> out) {
		ByteBufsParser<I> parser = ByteBufsParser.ofNullTerminatedBytes()
				.andThen(buf -> JsonUtils.fromJson(in, buf.asString(UTF_8)));
		return new ByteBufSerializer<I, O>() {
			@Override
			public ByteBuf serialize(O item) {
				ByteBufPoolAppendable appendable = new ByteBufPoolAppendable();
				JsonUtils.toJson(out, item, appendable);
				appendable.append("\0");
				return appendable.get();
			}

			@Nullable
			@Override
			public I tryParse(ByteBufQueue bufs) throws ParseException {
				return parser.tryParse(bufs);
			}
		};
	}
}
