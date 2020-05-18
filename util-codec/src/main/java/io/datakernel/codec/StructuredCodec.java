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

package io.datakernel.codec;

import io.datakernel.common.api.ParserFunction;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.common.exception.parse.ParseException;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A structured codec is an object that describes how some type T
 * can be written into {@link StructuredOutput} or read from {@link StructuredInput}.
 * The actual representation is abstracted away, so the same codec can be used to write or read
 * an object into/from various formats and protocols.
 */
public interface StructuredCodec<T> extends StructuredEncoder<T>, StructuredDecoder<T> {

	static <T> StructuredCodec<T> of(StructuredDecoder<T> decoder, StructuredEncoder<T> encoder) {
		return new StructuredCodec<T>() {
			@Override
			public void encode(StructuredOutput out, T item) {
				encoder.encode(out, item);
			}

			@Override
			public T decode(StructuredInput in) throws ParseException {
				return decoder.decode(in);
			}
		};
	}

	static <T> StructuredCodec<T> ofObject(StructuredDecoder<T> decoder, StructuredEncoder<T> encoder) {
		return of(StructuredDecoder.ofObject(decoder), StructuredEncoder.ofObject(encoder));
	}

	static <T> StructuredCodec<T> ofObject(Supplier<T> supplier) {
		return of(StructuredDecoder.ofObject(supplier), StructuredEncoder.ofObject());
	}

	static <T> StructuredCodec<T> ofTuple(StructuredDecoder<T> decoder, StructuredEncoder<T> encoder) {
		return of(StructuredDecoder.ofTuple(decoder), StructuredEncoder.ofTuple(encoder));
	}

	default StructuredCodec<@Nullable T> nullable() {
		return new StructuredCodec<T>() {
			@Nullable
			@Override
			public T decode(StructuredInput in) throws ParseException {
				return in.readNullable(StructuredCodec.this);
			}

			@Override
			public void encode(StructuredOutput out, T item) {
				out.writeNullable(StructuredCodec.this, item);
			}
		};
	}

	default StructuredCodec<List<T>> ofList() {
		return StructuredCodecs.ofList(this);
	}

	default <R> StructuredCodec<R> transform(ParserFunction<T, R> reader, Function<R, T> writer) {
		return new StructuredCodec<R>() {
			@Override
			public void encode(StructuredOutput out, R value) {
				T result = writer.apply(value);
				StructuredCodec.this.encode(out, result);
			}

			@Override
			public R decode(StructuredInput in) throws ParseException {
				T result = StructuredCodec.this.decode(in);
				try {
					return reader.parse(result);
				} catch (UncheckedException u) {
					throw u.propagate(ParseException.class);
				}
			}
		};
	}

}
