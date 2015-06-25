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

import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.asm.SerializerGenChar;

import static com.google.common.base.Preconditions.checkArgument;

public class DimensionTypeChar extends DimensionType implements DimensionTypeEnumerable {
	public DimensionTypeChar() {
		super(char.class);
	}

	@Override
	public SerializerGen serializerGen() {
		return new SerializerGenChar();
	}

	@Override
	public Object toInternalRepresentation(String o) {
		checkArgument(o.length() == 1);
		return o.charAt(0);
	}

	@Override
	public Object increment(Object object) {
		Character characterToIncrement = (Character) object;
		return ++characterToIncrement;
	}

	@Override
	public long difference(Object o1, Object o2) {
		return ((Character) o1) - ((Character) o2);
	}

	@Override
	public int compare(Object o1, Object o2) {
		return ((Character) o1).compareTo((Character) o2);
	}
}
