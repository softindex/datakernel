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

import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import static org.objectweb.asm.Type.VOID_TYPE;
import static org.objectweb.asm.Type.getType;

final class ExpressionThrow implements Expression {
	private final Class<? extends Throwable> exceptionClass;
	@Nullable
	private final Expression message;

	ExpressionThrow(Class<? extends Throwable> exceptionClass, @Nullable Expression message) {
		this.exceptionClass = exceptionClass;
		this.message = message;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		g.newInstance(getType(exceptionClass));
		g.dup();
		if (message == null) {
			g.invokeConstructor(getType(exceptionClass), new Method("<init>", VOID_TYPE, new Type[]{}));
		} else {
			message.load(ctx);
			g.invokeConstructor(getType(exceptionClass), new Method("<init>", VOID_TYPE, new Type[]{getType(String.class)}));
		}
		g.throwException();
		return VOID_TYPE;
	}
}
