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

import java.util.Objects;

import static io.datakernel.codegen.CompareOperation.*;
import static io.datakernel.codegen.Utils.isPrimitiveType;
import static io.datakernel.util.Preconditions.check;
import static org.objectweb.asm.Type.BOOLEAN_TYPE;
import static org.objectweb.asm.Type.INT_TYPE;

/**
 * Defines methods for comparing functions
 */
final class PredicateDefCmp implements PredicateDef {
	private final Expression left;
	private final Expression right;
	private final CompareOperation operation;

	// region builders
	PredicateDefCmp(CompareOperation operation, Expression left, Expression right) {
		this.left = left;
		this.right = right;
		this.operation = operation;
	}
	// endregion

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelTrue = new Label();
		Label labelExit = new Label();

		Type leftType = left.load(ctx);
		Type rightType = right.load(ctx);
		check(Objects.equals(leftType, rightType), "Types of compared values should match");

		if (isPrimitiveType(leftType)) {
			g.ifCmp(leftType, operation.opCode, labelTrue);
		} else {
			if (operation == EQ || operation == NE) {
				g.invokeVirtual(leftType, new Method("equals", BOOLEAN_TYPE, new Type[]{Type.getType(Object.class)}));
				g.push(operation == EQ);
				g.ifCmp(BOOLEAN_TYPE, GeneratorAdapter.EQ, labelTrue);
			} else {
				g.invokeVirtual(leftType, new Method("compareTo", INT_TYPE, new Type[]{Type.getType(Object.class)}));
				if (operation == LT) {
					g.ifZCmp(GeneratorAdapter.LT, labelTrue);
				} else if (operation == GT) {
					g.ifZCmp(GeneratorAdapter.GT, labelTrue);
				} else if (operation == LE) {
					g.ifZCmp(GeneratorAdapter.LE, labelTrue);
				} else if (operation == GE) {
					g.ifZCmp(GeneratorAdapter.GE, labelTrue);
				}
			}
		}

		g.push(false);
		g.goTo(labelExit);

		g.mark(labelTrue);
		g.push(true);

		g.mark(labelExit);

		return BOOLEAN_TYPE;
	}

	public int getOperationOpCode() {
		return operation.opCode;
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		PredicateDefCmp that = (PredicateDefCmp) o;

		if (!left.equals(that.left)) return false;
		if (!right.equals(that.right)) return false;
		if (operation != that.operation) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = left.hashCode();
		result = 31 * result + right.hashCode();
		result = 31 * result + operation.hashCode();
		return result;
	}
}
