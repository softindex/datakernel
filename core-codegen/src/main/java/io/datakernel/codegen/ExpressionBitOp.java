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

package io.datakernel.codegen;

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static org.objectweb.asm.Type.*;

final class ExpressionBitOp implements Expression {
	private final BitOperation op;
	private final Expression left;
	private final Expression right;

	ExpressionBitOp(BitOperation op, Expression left, Expression right) {
		this.op = op;
		this.left = left;
		this.right = right;
	}

	@Override
	public Type load(Context ctx) {
		if (op == BitOperation.AND || op == BitOperation.OR || op == BitOperation.XOR) {
			return loadOther(ctx);
		}
		return loadShift(ctx);
	}

	private Type loadOther(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Type leftType = left.load(ctx);
		Type rightType = right.load(ctx);

		Type resultType = unifyType(leftType.getSort(), rightType.getSort());

		if (!resultType.equals(leftType)) {
			int rightLocal = g.newLocal(rightType);
			g.storeLocal(rightLocal);
			g.cast(leftType, resultType);
			g.loadLocal(rightLocal);
		}

		if (!resultType.equals(rightType)) {
			g.cast(rightType, resultType);
		}

		g.visitInsn(resultType.getOpcode(op.opCode));
		return resultType;
	}

	private Type loadShift(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		VarLocal varIntShift = ctx.newLocal(INT_TYPE);
		Type rightType = right.load(ctx);
		switch (rightType.getSort()) {
			case BOOLEAN:
			case SHORT:
			case CHAR:
			case BYTE:
//				g.cast(right.type(ctx), Type.INT_TYPE);
//				break;
			case INT:
				break;
			default:
				throw new IllegalArgumentException("Right type is not an integral type");
		}
		varIntShift.store(ctx);

		Type leftType = left.load(ctx);
		int valueSort = leftType.getSort();

		if (valueSort == LONG) {
			varIntShift.load(ctx);
			g.visitInsn(LONG_TYPE.getOpcode(op.opCode));
			return LONG_TYPE;
		}

		if (valueSort == INT) {
			varIntShift.load(ctx);
			g.visitInsn(INT_TYPE.getOpcode(op.opCode));
			return INT_TYPE;
		}

		if (valueSort == BYTE || valueSort == SHORT || valueSort == CHAR || valueSort == BOOLEAN) {
//			g.cast(left.type(ctx), Type.INT_TYPE);
			varIntShift.load(ctx);

			g.visitInsn(INT_TYPE.getOpcode(op.opCode));
			return INT_TYPE;
		}

		throw new IllegalArgumentException("Left type if not an integral type");
	}

	private Type unifyType(int leftSort, int rightSort) {
		if (op == BitOperation.AND || op == BitOperation.OR || op == BitOperation.XOR) {
			if (leftSort == LONG || rightSort == LONG) return LONG_TYPE;
			if (leftSort == VOID || rightSort == VOID)
				throw new IllegalArgumentException("One of types of bit operation arguments is void");
			if (leftSort <= INT && rightSort <= INT) return INT_TYPE;
			throw new IllegalArgumentException("One of types of bit operation arguments is not integral");
		} else {
			if (rightSort == VOID || rightSort > INT)
				throw new IllegalArgumentException("Right argument of a bit operation arguments is void or not integral");
			if (leftSort == LONG) return LONG_TYPE;
			if (leftSort == VOID || leftSort > INT)
				throw new IllegalArgumentException("Left argument of a bit operation arguments is void or not integral");
			return INT_TYPE;
		}
	}
}
