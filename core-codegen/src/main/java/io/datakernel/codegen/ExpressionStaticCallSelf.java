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

import org.jetbrains.annotations.NotNull;
import org.objectweb.asm.Type;

import java.util.List;

final class ExpressionStaticCallSelf implements Expression {
	private final String methodName;
	private final List<Expression> arguments;

	public ExpressionStaticCallSelf(@NotNull String methodName, @NotNull List<Expression> expressions) {
		this.methodName = methodName;
		this.arguments = expressions;
	}

	@Override
	public Type load(Context ctx) {
		return ctx.invokeStatic(ctx.getSelfType(), methodName, arguments);
	}
}
