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
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.parse.ParserFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

public interface ByteBufsCodec<I, O> {
	ByteBuf encode(O item);

	@Nullable
	I tryDecode(ByteBufQueue bufs) throws ParseException;

	@NotNull
	default <I1, O1> ByteBufsCodec<I1, O1> andThen(ParserFunction<? super I, ? extends I1> decoder, Function<? super O1, ? extends O> encoder) {
		return new ByteBufsCodec<I1, O1>() {
			@Override
			public ByteBuf encode(O1 item) {
				return ByteBufsCodec.this.encode(encoder.apply(item));
			}

			@Nullable
			@Override
			public I1 tryDecode(ByteBufQueue bufs) throws ParseException {
				I maybeResult = ByteBufsCodec.this.tryDecode(bufs);
				if (maybeResult == null) return null;
				return decoder.parse(maybeResult);
			}
		};
	}

	@NotNull
	static ByteBufsCodec<ByteBuf, ByteBuf> ofDelimiter(ByteBufsDecoder<ByteBuf> delimiterIn, Function<ByteBuf, ByteBuf> delimiterOut) {
		return new ByteBufsCodec<ByteBuf, ByteBuf>() {
			@Override
			public ByteBuf encode(ByteBuf buf) {
				return delimiterOut.apply(buf);
			}

			@Nullable
			@Override
			public ByteBuf tryDecode(ByteBufQueue bufs) throws ParseException {
				return delimiterIn.tryDecode(bufs);
			}
		};
	}

}
