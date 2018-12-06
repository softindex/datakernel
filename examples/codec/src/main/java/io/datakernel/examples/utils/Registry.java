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

package io.datakernel.examples.utils;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecRegistry;

import java.time.LocalDate;

import static io.datakernel.codec.StructuredCodecs.*;

public class Registry {
	public static final CodecRegistry REGISTRY = CodecRegistry.create()
			.with(LocalDate.class, StructuredCodec.of(
					in -> LocalDate.parse(in.readString()),
					(out, item) -> out.writeString(item.toString())))
			.with(Person.class, registry -> object(Person::new,
					"id", Person::getId, INT_CODEC,
					"name", Person::getName, STRING_CODEC,
					"date of birth", Person::getDateOfBirth, registry.get(LocalDate.class)));
}
