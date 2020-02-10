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

package io.datakernel.aggregation.measure;

import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.codegen.Context;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.SerializerDef;
import io.datakernel.serializer.impl.SerializerDefArray;
import io.datakernel.serializer.impl.SerializerDefByte;
import io.datakernel.serializer.impl.SerializerDefClass;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.Utils.isWrapperType;
import static java.util.Collections.singletonList;
import static org.objectweb.asm.Type.*;

public final class MeasureHyperLogLog extends Measure {
	private final int registers;

	private static final class FieldTypeHyperLogLog extends FieldType<Integer> {
		public FieldTypeHyperLogLog() {
			super(HyperLogLog.class, int.class, serializerDef(), INT_CODEC, null);
		}

		private static SerializerDef serializerDef() {
			SerializerDefClass serializer = SerializerDefClass.of(HyperLogLog.class);
			try {
				serializer.addGetter(HyperLogLog.class.getMethod("getRegisters"),
						new SerializerDefArray(new SerializerDefByte(false), byte[].class), -1, -1);
				serializer.setConstructor(HyperLogLog.class.getConstructor(byte[].class),
						singletonList("registers"));
			} catch (NoSuchMethodException ignored) {
				throw new RuntimeException("Unable to construct SerializerDef for HyperLogLog");
			}
			return serializer;
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
	public Expression zeroAccumulator(Variable accumulator) {
		return set(accumulator, constructor(HyperLogLog.class, value(registers)));
	}

	@Override
	public Expression initAccumulatorWithAccumulator(Variable accumulator, Expression firstAccumulator) {
		return sequence(
				set(accumulator, constructor(HyperLogLog.class, value(registers))),
				call(accumulator, "union", firstAccumulator));
	}

	@Override
	public Expression reduce(Variable accumulator,
			Variable nextAccumulator) {
		return call(accumulator, "union", nextAccumulator);
	}

	@Override
	public Expression initAccumulatorWithValue(Variable accumulator,
			Variable firstValue) {
		return sequence(
				set(accumulator, constructor(HyperLogLog.class, value(registers))),
				add(accumulator, firstValue));
	}

	@Override
	public Expression accumulate(Variable accumulator, Variable nextValue) {
		return add(accumulator, nextValue);
	}

	private static Expression add(Expression accumulator, Expression value) {
		return new ExpressionHyperLogLog(value, accumulator);
	}

	private static class ExpressionHyperLogLog implements Expression {
		private final Expression value;
		private final Expression accumulator;

		public ExpressionHyperLogLog(Expression value, Expression accumulator) {
			this.value = value;
			this.accumulator = accumulator;
		}

		@Override
		public Type load(Context ctx) {
			GeneratorAdapter g = ctx.getGeneratorAdapter();
			Type accumulatorType = accumulator.load(ctx);
			Type valueType = value.load(ctx);
			String methodName;
			Type methodParameterType;
			if (valueType == LONG_TYPE || valueType.getClassName().equals(Long.class.getName())) {
				methodName = "addLong";
				methodParameterType = LONG_TYPE;
			} else if (valueType == INT_TYPE || valueType.getClassName().equals(Integer.class.getName())) {
				methodName = "addInt";
				methodParameterType = INT_TYPE;
			} else {
				methodName = "addObject";
				methodParameterType = getType(Object.class);
			}

			if (isWrapperType(valueType)) {
				g.unbox(methodParameterType);
			}

			ctx.invoke(accumulatorType, methodName, methodParameterType);

			return VOID_TYPE;
		}
	}
}
