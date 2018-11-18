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

import io.datakernel.exception.ParseException;
import io.datakernel.util.Initializable;
import io.datakernel.util.Tuple2;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.max;

public final class CodecSubtype<T> implements Initializable<CodecSubtype<T>>, StructuredCodec<T> {
	private final Map<String, StructuredCodec<? extends T>> namesToAdapters = new HashMap<>();
	private final Map<Type, String> subtypesToNames = new HashMap<>();

	private CodecSubtype() {
	}

	public static <T> CodecSubtype<T> create() {
		return new CodecSubtype<>();
	}

	public CodecSubtype<T> with(Type type, String name, StructuredCodec<? extends T> adapter) {
		namesToAdapters.put(name, adapter);
		subtypesToNames.put(type, name);
		return this;
	}

	public CodecSubtype<T> with(Type type, StructuredCodec<? extends T> adapter) {
		String name = type.getTypeName();
		name = name.substring(max(name.lastIndexOf('.'), name.lastIndexOf('$')) + 1);
		return with(type, name, adapter);
	}

	@SuppressWarnings({"unchecked"})
	@Override
	public void encode(StructuredOutput out, T value) {
		StructuredOutput.MapWriter mapWriter = out.mapWriter(false);
		String field = subtypesToNames.get(value.getClass());
		StructuredCodec<T> codec = (StructuredCodec<T>) namesToAdapters.get(field);
		codec.encode(mapWriter.next(field), value);
		mapWriter.close();
	}

	@SuppressWarnings("unchecked")
	@Override
	public T decode(StructuredInput in) throws ParseException {
		StructuredInput.MapReader mapReader = in.mapReader(false);
		Tuple2<String, StructuredInput> entry = mapReader.next();
		StructuredCodec<? extends T> codec = namesToAdapters.get(entry.getValue1());
		T result = codec.decode(entry.getValue2());
		mapReader.close();
		return result;
	}
}
