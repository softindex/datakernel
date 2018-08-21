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

package io.datakernel.codegen;

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.datakernel.codegen.Utils.isValidCast;
import static io.datakernel.codegen.Utils.newLocal;
import static io.datakernel.util.Preconditions.checkNotNull;

final class ExpressionBitOp implements Expression {
	private final BitOperation op;
	private final Expression left;
	private final Expression right;

	ExpressionBitOp(BitOperation op, Expression left, Expression right) {
		this.op = checkNotNull(op);
		this.left = checkNotNull(left);
		this.right = checkNotNull(right);
	}

	@Override
	public Type type(Context ctx) {
		int leftSort = left.type(ctx).getSort();
		int rightSort = right.type(ctx).getSort();

		if (op == BitOperation.AND || op == BitOperation.OR || op == BitOperation.XOR) {
			if (leftSort == Type.LONG || rightSort == Type.LONG) return Type.LONG_TYPE;
			if (leftSort == Type.VOID || rightSort == Type.VOID) throw new IllegalArgumentException();
			if (leftSort <= Type.INT && rightSort <= Type.INT) return Type.INT_TYPE;
			throw new IllegalArgumentException();
		} else {
			if (rightSort == Type.VOID || rightSort > Type.INT) throw new IllegalArgumentException();
			if (leftSort == Type.LONG) return Type.LONG_TYPE;
			if (leftSort == Type.VOID || leftSort > Type.INT) throw new IllegalArgumentException();
			return Type.INT_TYPE;
		}
	}

	@Override
	public Type load(Context ctx) {
		if (op == BitOperation.AND || op == BitOperation.OR || op == BitOperation.XOR) {
			return loadOther(ctx);
		} else {
			return loadShift(ctx);
		}
	}

	private Type loadOther(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type type = type(ctx);

		left.load(ctx);
		Type leftType = left.type(ctx);
		if (isValidCast(leftType, type)) {
			g.cast(leftType, type);
		}

		right.load(ctx);
		Type rightType = right.type(ctx);
		if (isValidCast(rightType, type)) {
			g.cast(rightType, type);
		}

		g.visitInsn(type.getOpcode(op.opCode));
		return type;
	}

	private Type loadShift(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		VarLocal varIntShift = newLocal(ctx, Type.INT_TYPE);
		right.load(ctx);
		switch (right.type(ctx).getSort()) {
			case Type.BOOLEAN:
			case Type.SHORT:
			case Type.CHAR:
			case Type.BYTE:
//				g.cast(right.type(ctx), Type.INT_TYPE);
//				break;
			case Type.INT:
				break;
			default:
				throw new IllegalArgumentException();
		}
		varIntShift.store(ctx);

		left.load(ctx);
		int valueSort = left.type(ctx).getSort();

		if (valueSort == Type.LONG) {
			varIntShift.load(ctx);
			g.visitInsn(Type.LONG_TYPE.getOpcode(op.opCode));
			return Type.LONG_TYPE;
		}

		if (valueSort == Type.INT) {
			varIntShift.load(ctx);
			g.visitInsn(Type.INT_TYPE.getOpcode(op.opCode));
			return Type.INT_TYPE;
		}

		if (valueSort == Type.BYTE || valueSort == Type.SHORT || valueSort == Type.CHAR || valueSort == Type.BOOLEAN) {
//			g.cast(left.type(ctx), Type.INT_TYPE);
			varIntShift.load(ctx);

			g.visitInsn(Type.INT_TYPE.getOpcode(op.opCode));
			return Type.INT_TYPE;
		}

		throw new IllegalArgumentException();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionBitOp that = (ExpressionBitOp) o;

		if (op != that.op) return false;
		if (!left.equals(that.left)) return false;
		if (!right.equals(that.right)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = op.hashCode();
		result = 31 * result + left.hashCode();
		result = 31 * result + right.hashCode();
		return result;
	}
}
