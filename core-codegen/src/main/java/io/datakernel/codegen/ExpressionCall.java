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

import java.util.Arrays;
import java.util.Objects;

/**
 * Defines methods for using static methods from other classes
 */
final class ExpressionCall implements Expression {
	private final Expression owner;
	private final String methodName;
	private final Expression[] arguments;

	ExpressionCall(Expression owner, String methodName, Expression[] arguments) {
		this.owner = owner;
		this.methodName = methodName;
		this.arguments = arguments;
	}

	@Override
	public Type load(Context ctx) {
		return ctx.invoke(owner, methodName, arguments);
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ExpressionCall that = (ExpressionCall) o;
		if (!Objects.equals(this.owner, that.owner)) return false;
		if (!Objects.equals(this.methodName, that.methodName)) return false;
		if (!Arrays.equals(this.arguments, that.arguments)) return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = owner.hashCode();
		result = 31 * result + methodName.hashCode();
		result = 31 * result + Arrays.hashCode(arguments);
		return result;
	}
}
