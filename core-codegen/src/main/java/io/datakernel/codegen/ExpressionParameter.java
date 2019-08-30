package io.datakernel.codegen;

import org.objectweb.asm.Type;

import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class ExpressionParameter implements Expression {
	private final String name;
	private Expression expression;

	ExpressionParameter(String name) {
		this.name = checkNotNull(name);
	}

	public static Expression bind(String name, Function<ExpressionParameter, Expression> fn) {
		ExpressionParameter expressionParameter = new ExpressionParameter(name);
		Expression result = fn.apply(expressionParameter);
		return expressionParameter.new BoundExpression(result);
	}

	private final class BoundExpression implements Expression {
		private final Expression result;

		public BoundExpression(Expression result) {
			this.result = checkNotNull(result);
		}

		@Override
		public Type load(Context ctx) {
			ensureExpression(ctx);
			return result.load(ctx);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			BoundExpression that = (BoundExpression) o;
			return result.equals(that.result);
		}

		@Override
		public int hashCode() {
			return result.hashCode();
		}
	}

	private Expression ensureExpression(Context ctx) {
		if (expression == null) {
			expression = ctx.removeParameter(name);
		}
		return expression;
	}

	@Override
	public Type load(Context ctx) {
		return ensureExpression(ctx).load(ctx);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionParameter that = (ExpressionParameter) o;
		return name.equals(that.name);
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}
}
