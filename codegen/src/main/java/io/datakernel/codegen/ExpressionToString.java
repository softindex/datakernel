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
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Defines methods which allow to create a string
 */
@SuppressWarnings("PointlessArithmeticExpression")
public final class ExpressionToString implements Expression {
	private String begin = "{";
	private String end = "}";
	private String separator = " ";
	private final Map<Object, Expression> arguments = new LinkedHashMap<>();

	ExpressionToString() {
	}

	public static ExpressionToString create() {
		return new ExpressionToString();
	}

	public ExpressionToString withArgument(String label, Expression expression) {
		this.arguments.put(checkNotNull(label), checkNotNull(expression));
		return this;
	}

	public ExpressionToString withArgument(Expression expression) {
		this.arguments.put(arguments.size() + 1, checkNotNull(expression));
		return this;
	}

	public ExpressionToString withSeparator(String separator) {
		this.separator = checkNotNull(separator);
		return this;
	}

	public ExpressionToString withQuotes(String begin, String end) {
		this.begin = checkNotNull(begin);
		this.end = checkNotNull(end);
		return this;
	}

	public ExpressionToString withQuotes(String begin, String end, String separator) {
		this.begin = checkNotNull(begin);
		this.end = checkNotNull(end);
		this.separator = checkNotNull(separator);
		return this;
	}

	@Override
	public Type type(Context ctx) {
		return getType(String.class);
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

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
			Expression expression = arguments.get(key);
			Type type = expression.load(ctx);
			if (isPrimitiveType(type)) {
				g.invokeStatic(wrap(type), new Method("toString", getType(String.class), new Type[]{type}));
			} else {
				Label nullLabel = new Label();
				Label afterToString = new Label();
				g.dup();
				g.ifNull(nullLabel);
				g.invokeVirtual(type, getMethod("String toString()"));
				g.goTo(afterToString);
				g.mark(nullLabel);
				g.pop();
				g.push("null");
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

		if (!begin.equals(that.begin)) return false;
		if (!end.equals(that.end)) return false;
		if (!separator.equals(that.separator)) return false;
		if (!arguments.equals(that.arguments)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = begin.hashCode();
		result = 31 * result + end.hashCode();
		result = 31 * result + separator.hashCode();
		result = 31 * result + arguments.hashCode();
		return result;
	}
}
