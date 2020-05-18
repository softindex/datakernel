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

package io.datakernel.codegen.expression;

import io.datakernel.codegen.Context;
import org.jetbrains.annotations.NotNull;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

final class ExpressionIsNull implements Expression {
	private final Expression expression;

	ExpressionIsNull(@NotNull Expression expression) {
		this.expression = expression;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Label labelNull = new Label();
		Label labelExit = new Label();

		expression.load(ctx);
		g.ifNull(labelNull);
		g.push(false);
		g.goTo(labelExit);

		g.mark(labelNull);
		g.push(true);

		g.mark(labelExit);

		return Type.BOOLEAN_TYPE;
	}
}
