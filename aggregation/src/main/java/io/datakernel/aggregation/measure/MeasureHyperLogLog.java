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

package io.datakernel.aggregation.measure;

import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.codegen.Context;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.VarField;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.asm.SerializerGenArray;
import io.datakernel.serializer.asm.SerializerGenByte;
import io.datakernel.serializer.asm.SerializerGenClass;
import org.objectweb.asm.Type;

import static io.datakernel.codegen.Expressions.*;
import static java.util.Collections.singletonList;

public final class MeasureHyperLogLog extends Measure {
	private final int registers;

	private static final class FieldTypeHyperLogLog extends FieldType<Integer> {
		public FieldTypeHyperLogLog() {
			super(HyperLogLog.class, int.class, serializerGen());
		}

		private static SerializerGen serializerGen() {
			SerializerGenClass serializerGenClass = new SerializerGenClass(HyperLogLog.class);
			try {
				serializerGenClass.addGetter(HyperLogLog.class.getMethod("getRegisters"),
						new SerializerGenArray(new SerializerGenByte(), byte[].class), -1, -1);
				serializerGenClass.setConstructor(HyperLogLog.class.getConstructor(byte[].class),
						singletonList("registers"));
			} catch (NoSuchMethodException e) {
				throw new RuntimeException("Unable to construct SerializerGen for HyperLogLog");
			}
			return serializerGenClass;
		}
	}

	MeasureHyperLogLog(int registers) {
		super(new FieldTypeHyperLogLog());
		this.registers = registers;
	}

	public static MeasureHyperLogLog create(int registers) {
		return new MeasureHyperLogLog(registers);
	}

	@Override
	public Expression valueOfAccumulator(Expression accumulator) {
		return call(accumulator, "estimate");
	}

	@Override
	public Expression zeroAccumulator(VarField accumulator) {
		return set(accumulator, constructor(HyperLogLog.class, value(registers)));
	}

	@Override
	public Expression initAccumulatorWithAccumulator(VarField accumulator,
	                                                 Expression firstAccumulator) {
		return sequence(
				set(accumulator, constructor(HyperLogLog.class, value(registers))),
				call(accumulator, "union", firstAccumulator));
	}

	@Override
	public Expression reduce(VarField accumulator,
	                         VarField nextAccumulator) {
		return call(accumulator, "union", nextAccumulator);
	}

	@Override
	public Expression initAccumulatorWithValue(VarField accumulator,
	                                           VarField firstValue) {
		return sequence(
				set(accumulator, constructor(HyperLogLog.class, value(registers))),
				add(accumulator, firstValue));
	}

	@Override
	public Expression accumulate(VarField accumulator, VarField nextValue) {
		return add(accumulator, nextValue);
	}

	private static Expression add(final Expression accumulator, final Expression value) {
		return new Expression() {
			@Override
			public Type type(Context ctx) {
				return Type.VOID_TYPE;
			}

			@Override
			public Type load(Context ctx) {
				Type valueType = value.type(ctx);
				if (valueType == Type.LONG_TYPE || valueType.getClassName().equals(Long.class.getName())) {
					call(accumulator, "addLong", value).load(ctx);
				} else if (valueType == Type.INT_TYPE || valueType.getClassName().equals(Integer.class.getName())) {
					call(accumulator, "addInt", value).load(ctx);
				} else {
					call(accumulator, "addObject", value).load(ctx);
				}
				return Type.VOID_TYPE;
			}
		};
	}
}
