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

import static io.datakernel.codegen.Utils.newLocal;
import static org.objectweb.asm.Opcodes.*;

final class ExpressionBitOp implements Expression {
	private final Operation op;
	private final Expression left;
	private final Expression right;

	ExpressionBitOp(Operation op, Expression left, Expression right) {
		this.op = op;
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionBitOp that = (ExpressionBitOp) o;

		if (op != that.op) return false;
		if (left != null ? !left.equals(that.left) : that.left != null) return false;
		return !(right != null ? !right.equals(that.right) : that.right != null);

	}

	@Override
	public int hashCode() {
		int result = op != null ? op.hashCode() : 0;
		result = 31 * result + (left != null ? left.hashCode() : 0);
		result = 31 * result + (right != null ? right.hashCode() : 0);
		return result;
	}

	public enum Operation {
		SHL(ISHL, "<<"), SHR(ISHR, ">>"), USHR(IUSHR, ">>>"), AND(IAND, "&"), OR(IOR, "|"), XOR(IXOR, "^");

		private final int opCode;
		private final String symbol;

		Operation(int opCode, String symbol) {
			this.opCode = opCode;
			this.symbol = symbol;
		}

		public static Operation operation(String symbol) {
			for (Operation operation : Operation.values()) {
				if (operation.symbol.equals(symbol)) {
					return operation;
				}
			}
			throw new IllegalArgumentException();
		}
	}

	@Override
	public Type type(Context ctx) {
		int leftSort = left.type(ctx).getSort();
		int rightSort = right.type(ctx).getSort();

		if (op == Operation.AND || op == Operation.OR || op == Operation.XOR) {
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
		if (op == Operation.AND || op == Operation.OR || op == Operation.XOR) {
			return loadOther(ctx);
		} else {
			return loadShift(ctx);
		}
	}

	private Type loadOther(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type type = type(ctx);

		left.load(ctx);
		if (!left.type(ctx).equals(type)) {
			g.cast(left.type(ctx), type);
		}

		right.load(ctx);
		if (!right.type(ctx).equals(type)) {
			g.cast(right.type(ctx), type);
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
				g.cast(right.type(ctx), Type.INT_TYPE);
				break;
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
			g.cast(left.type(ctx), Type.INT_TYPE);
			varIntShift.load(ctx);

			g.visitInsn(Type.INT_TYPE.getOpcode(op.opCode));
			return Type.INT_TYPE;
		}

		throw new IllegalArgumentException();
	}
}
