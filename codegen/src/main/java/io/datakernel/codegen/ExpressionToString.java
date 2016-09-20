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

import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.codegen.Utils.isPrimitiveType;
import static io.datakernel.codegen.Utils.wrap;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Defines methods which allow to create a string
 */
@SuppressWarnings("PointlessArithmeticExpression")
final class ExpressionToString implements Expression {
	private String begin = "{";
	private String end = "}";
	private String separator = " ";
	private final Map<Object, Expression> arguments = new LinkedHashMap<>();

	ExpressionToString(Map<String, Expression> arguments) {
		this.arguments.putAll(arguments);
	}

	ExpressionToString() {
	}

	public ExpressionToString add(String label, Expression expression) {
		this.arguments.put(label, expression);
		return this;
	}

	public ExpressionToString add(Expression expression) {
		this.arguments.put(arguments.size() + 1, expression);
		return this;
	}

	public ExpressionToString separator(String separator) {
		this.separator = separator;
		return this;
	}

	public ExpressionToString quotes(String begin, String end) {
		this.begin = begin;
		this.end = end;
		return this;
	}

	public ExpressionToString quotes(String begin, String end, String separator) {
		this.begin = begin;
		this.end = end;
		this.separator = separator;
		return this;
	}

	@Override
	public Type type(Context ctx) {
		return getType(String.class);
	}

	@Override
	public Type load(Context ctx) {
		final GeneratorAdapter g = ctx.getGeneratorAdapter();

		g.newInstance(getType(StringBuilder.class));
		g.dup();
		g.invokeConstructor(getType(StringBuilder.class), getMethod("void <init> ()"));

		boolean first = true;

		for (Object key : arguments.keySet()) {
			String str = first ? begin : separator;
			first = false;
			if (key instanceof String) {
				str += key;
			}
			if (!str.isEmpty()) {
				g.dup();
				g.push(str);
				g.invokeVirtual(getType(StringBuilder.class), getMethod("StringBuilder append(String)"));
				g.pop();
			}

			g.dup();
			final Expression expression = arguments.get(key);
			final Type type = expression.load(ctx);
			if (isPrimitiveType(type)) {
				g.invokeStatic(wrap(type), new Method("toString", getType(String.class), new Type[]{type}));
			} else {
				final Label nullLabel = new Label();
				final Label afterToString = new Label();
				g.dup();
				g.ifNull(nullLabel);
				g.invokeVirtual(type, getMethod("String toString()"));
				g.goTo(afterToString);
				g.mark(nullLabel);
				g.pop();
				g.push(("null"));
				g.mark(afterToString);
			}
			g.invokeVirtual(getType(StringBuilder.class), getMethod("StringBuilder append(String)"));
			g.pop();
		}

		if (!end.isEmpty()) {
			g.dup();
			g.push(end);
			g.invokeVirtual(getType(StringBuilder.class), getMethod("StringBuilder append(String)"));
			g.pop();
		}

		g.invokeVirtual(getType(StringBuilder.class), getMethod("String toString()"));
		return type(ctx);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionToString that = (ExpressionToString) o;

		if (arguments != null ? !arguments.equals(that.arguments) : that.arguments != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return (arguments != null ? arguments.hashCode() : 0);
	}
}
