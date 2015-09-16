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

import io.datakernel.codegen.utils.Preconditions;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.codegen.Utils.isPrimitiveType;
import static io.datakernel.codegen.Utils.wrap;
import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.commons.GeneratorAdapter.NE;

/**
 * Defines methods to compare some fields
 */
public final class ExpressionComparator implements Expression {
	private final List<Expression> left = new ArrayList<>();
	private final List<Expression> right = new ArrayList<>();

	ExpressionComparator() {
	}

	ExpressionComparator(List<Expression> left, List<Expression> right) {
		Preconditions.check(left.size() == right.size());
		this.left.addAll(left);
		this.right.addAll(right);
	}

	public ExpressionComparator add(Expression left, Expression right) {
		this.left.add(left);
		this.right.add(right);
		return this;
	}

	@Override
	public Type type(Context ctx) {
		return INT_TYPE;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Label labelNe = new Label();

		for (int i = 0; i < left.size(); i++) {
			Type leftFieldType = left.get(i).load(ctx);
			Type rightFieldType = right.get(i).load(ctx);

			Preconditions.check(leftFieldType.equals(rightFieldType));
			if (isPrimitiveType(leftFieldType)) {
				g.invokeStatic(wrap(leftFieldType), new Method("compare", INT_TYPE, new Type[]{leftFieldType, leftFieldType}));
				g.dup();
				g.ifZCmp(NE, labelNe);
				g.pop();
			} else {
				g.invokeVirtual(leftFieldType, new Method("compareTo", INT_TYPE, new Type[]{Type.getType(Object.class)}));
				g.dup();
				g.ifZCmp(NE, labelNe);
				g.pop();
			}
		}

		g.push(0);

		g.mark(labelNe);

		return INT_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionComparator that = (ExpressionComparator) o;

		if (left != null ? !left.equals(that.left) : that.left != null) return false;
		if (right != null ? !right.equals(that.right) : that.right != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = left != null ? left.hashCode() : 0;
		result = 31 * result + (right != null ? right.hashCode() : 0);
		return result;
	}
}
