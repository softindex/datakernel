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

import static io.datakernel.codegen.Utils.*;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.getType;

/**
 * Defines arithmetic operations for the functions
 */
final class ExpressionArithmeticOp implements Expression {
	ExpressionArithmeticOp(Operation op, Expression left, Expression right) {
		this.op = op;
		this.left = left;
		this.right = right;
	}

	public enum Operation {
		ADD(IADD, "+"), SUB(ISUB, "-"), MUL(IMUL, "*"), DIV(IDIV, "/"), REM(IREM, "%");

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

	private final Operation op;
	private final Expression left;
	private final Expression right;

	public static Class<?> unifyArithmeticTypes(Class<?>... dataTypes) {
		Class<?> resultType = null;
		int resultOrder = 0;

		for (Class<?> dataType : dataTypes) {
			Class<?> t;
			int order;
			if (dataType == Byte.TYPE ||
					dataType == Short.TYPE ||
					dataType == Character.TYPE ||
					dataType == Integer.TYPE) {
				t = Integer.TYPE;
				order = 1;
			} else if (dataType == Long.TYPE) {
				t = Long.TYPE;
				order = 2;
			} else if (dataType == Float.TYPE) {
				t = Float.TYPE;
				order = 3;
			} else if (dataType == Double.TYPE) {
				t = Double.TYPE;
				order = 4;
			} else
				throw new IllegalArgumentException();
			if (resultType == null || order > resultOrder) {
				resultType = t;
				resultOrder = order;
			}
		}

		return resultType;
	}

	@Override
	public Type type(Context ctx) {
		Type leftType = left.type(ctx);
		Type rightType = right.type(ctx);
		if (isWrapperType(leftType)) {
			leftType = unwrap(leftType);
		}
		if (isWrapperType(rightType)) {
			rightType = unwrap(rightType);
		}
		return getType(unifyArithmeticTypes(getJavaType(leftType), getJavaType(rightType)));
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Expression leftVar = left;
		Expression rightVar = right;
		if (isWrapperType(leftVar.type(ctx))) {
			leftVar.load(ctx);
			g.unbox(unwrap(leftVar.type(ctx)));
			VarLocal newLeftVar = newLocal(ctx, unwrap(leftVar.type(ctx)));
			newLeftVar.storeLocal(g);
			leftVar = newLeftVar;
		}
		if (isWrapperType(rightVar.type(ctx))) {
			rightVar.load(ctx);
			g.unbox(unwrap(rightVar.type(ctx)));
			VarLocal newRightVar = newLocal(ctx, unwrap(rightVar.type(ctx)));
			newRightVar.storeLocal(g);
			rightVar = newRightVar;
		}
		Type resultType = getType(unifyArithmeticTypes(
				getJavaType(leftVar.type(ctx)), getJavaType(rightVar.type(ctx))));
		if (leftVar.type(ctx) != resultType) {
			leftVar.load(ctx);
			g.cast(leftVar.type(ctx), resultType);
			VarLocal newLeftVar = newLocal(ctx, resultType);
			newLeftVar.storeLocal(g);
			leftVar = newLeftVar;
		}
		if (rightVar.type(ctx) != resultType) {
			rightVar.load(ctx);
			g.cast(rightVar.type(ctx), resultType);
			VarLocal newRightVar = newLocal(ctx, resultType);
			newRightVar.storeLocal(g);
			rightVar = newRightVar;
		}
		leftVar.load(ctx);
		rightVar.load(ctx);
		g.visitInsn(resultType.getOpcode(op.opCode));
		return resultType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionArithmeticOp that = (ExpressionArithmeticOp) o;

		if (left != null ? !left.equals(that.left) : that.left != null) return false;
		if (op != that.op) return false;
		if (right != null ? !right.equals(that.right) : that.right != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = op != null ? op.hashCode() : 0;
		result = 31 * result + (left != null ? left.hashCode() : 0);
		result = 31 * result + (right != null ? right.hashCode() : 0);
		return result;
	}

}
