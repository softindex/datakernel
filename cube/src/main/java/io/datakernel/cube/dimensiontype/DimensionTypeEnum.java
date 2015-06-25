/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.cube.dimensiontype;

import com.google.common.collect.Lists;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.asm.SerializerGenByte;

import java.util.List;

public class DimensionTypeEnum extends DimensionType {
	private final List<String> enumNames = Lists.newArrayList();

	public DimensionTypeEnum(Class<? extends Enum<?>> enumClass) {
		super(byte.class);
		for (Enum<?> enumConstant : enumClass.getEnumConstants()) {
			enumNames.add(enumConstant.name());
		}
	}

	@Override
	public SerializerGen serializerGen() {
		return new SerializerGenByte();
	}

	@Override
	public Object toInternalRepresentation(String enumName) {
		return enumNames.indexOf(enumName);
	}

	@Override
	public String toString(Object ordinal) {
		return enumNames.get((Integer) ordinal);
	}

	@Override
	public int compare(Object ordinal1, Object ordinal2) {
		return Integer.compare((Integer) ordinal1, (Integer) ordinal2);
	}
}
