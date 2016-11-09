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

import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.codegen.ExpressionCast.THIS_TYPE;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.Expressions.cast;
import static io.datakernel.codegen.Utils.*;
import static io.datakernel.codegen.utils.Preconditions.check;
import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.commons.GeneratorAdapter.NE;

/**
 * Defines methods to compare some fields
 */
public final class ExpressionComparator implements Expression {
	private static final class ComparablePair {
		private final Expression left;
		private final Expression right;
		private final boolean nullable;

		private ComparablePair(Expression left, Expression right, boolean nullable) {
			this.left = left;
			this.right = right;
			this.nullable = nullable;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ComparablePair that = (ComparablePair) o;

			if (nullable != that.nullable) return false;
			if (!left.equals(that.left)) return false;
			return right.equals(that.right);

		}

		@Override
		public int hashCode() {
			int result = left.hashCode();
			result = 31 * result + right.hashCode();
			result = 31 * result + (nullable ? 1 : 0);
			return result;
		}
	}

	private final List<ComparablePair> pairs = new ArrayList<>();

	private ExpressionComparator() {
	}

	public static ExpressionComparator create() {
		return new ExpressionComparator();
	}

	public ExpressionComparator with(Expression left, Expression right) {
		return with(left, right, false);
	}

	public ExpressionComparator with(Expression left, Expression right, boolean nullable) {
		this.pairs.add(new ComparablePair(left, right, nullable));
		return this;
	}

	public static Expression thisField(String field) {
		return field(self(), field);
	}

	public static Expression thatField(String field) {
		return field(cast(arg(0), THIS_TYPE), field);
	}

	public static Expression leftField(Class<?> type, String field) {
		return field(cast(arg(0), type), field);
	}

	public static Expression rightField(Class<?> type, String field) {
		return field(cast(arg(1), type), field);
	}

	@Override
	public Type type(Context ctx) {
		return INT_TYPE;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Label labelReturn = new Label();

		for (ComparablePair pair : pairs) {
			Type leftFieldType = pair.left.load(ctx);
			Type rightFieldType = pair.right.load(ctx);

			check(leftFieldType.equals(rightFieldType));
			if (isPrimitiveType(leftFieldType)) {
				g.invokeStatic(wrap(leftFieldType), new Method("compare", INT_TYPE, new Type[]{leftFieldType, leftFieldType}));
				g.dup();
				g.ifZCmp(NE, labelReturn);
				g.pop();
			} else if (!pair.nullable) {
				g.invokeVirtual(leftFieldType, new Method("compareTo", INT_TYPE, new Type[]{Type.getType(Object.class)}));
				g.dup();
				g.ifZCmp(NE, labelReturn);
				g.pop();
			} else {
				VarLocal varRight = newLocal(ctx, rightFieldType);
				varRight.store(ctx);

				VarLocal varLeft = newLocal(ctx, leftFieldType);
				varLeft.store(ctx);

				Label continueLabel = new Label();
				Label nonNulls = new Label();
				Label leftNonNull = new Label();

				varLeft.load(ctx);
				g.ifNonNull(leftNonNull);

				varRight.load(ctx);
				g.ifNull(continueLabel);
				g.push(-1);
				g.returnValue();

				g.mark(leftNonNull);

				varRight.load(ctx);
				g.ifNonNull(nonNulls);
				g.push(1);
				g.returnValue();

				g.mark(nonNulls);

				varLeft.load(ctx);
				varRight.load(ctx);

				g.invokeVirtual(leftFieldType, new Method("compareTo", INT_TYPE, new Type[]{Type.getType(Object.class)}));
				g.dup();
				g.ifZCmp(NE, labelReturn);
				g.pop();

				g.mark(continueLabel);
			}
		}

		g.push(0);

		g.mark(labelReturn);

		return INT_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionComparator that = (ExpressionComparator) o;

		if (!pairs.equals(that.pairs)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return pairs.hashCode();
	}
}
