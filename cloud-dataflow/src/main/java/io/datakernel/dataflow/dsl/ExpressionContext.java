package io.datakernel.dataflow.dsl;

import io.datakernel.dataflow.dataset.Dataset;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public final class ExpressionContext {
	private final EvaluationContext evaluationContext;
	private final List<Object> items;

	public ExpressionContext(EvaluationContext evaluationContext, List<Object> items) {
		this.evaluationContext = evaluationContext;
		this.items = items;
	}

	public EvaluationContext getEvaluationContext() {
		return evaluationContext;
	}

	private <T> T getItem(int position, Class<T> cls, String name) {
		if (position < 0 || position >= items.size()) {
			throw new IndexOutOfBoundsException("Index " + position + " is out of bounds [0; " + items.size() + "]");
		}
		Object object = items.get(position);
		if (!cls.isInstance(object)) {
			throw new IllegalStateException("Tried to get " + name + " at index " + position + ", got " + object);
		}
		return cls.cast(object);
	}

	public int getInt(int position) {
		return getItem(position, int.class, "an int");
	}

	public String getString(int position) {
		return getItem(position, String.class, "a string");
	}

	public Class<Object> getClass(int position) {
		return evaluationContext.resolveClass(getString(position));
	}

	public <T> T generateInstance(int position) {
		return evaluationContext.generateInstance(getString(position));
	}

	public <T> Dataset<T> evaluateExpr(int position) {
		return getItem(position, AST.Expression.class, "an expression").evaluate(evaluationContext);
	}

	@Nullable
	@SuppressWarnings("unchecked")
	public ExpressionContext getOptionalGroup(int position) {
		Object object = items.get(position);
		if (object == ExpressionDef.NO_GROUP) {
			return null;
		}
		if (!(object instanceof List)) {
			throw new IllegalStateException("Tried to get an optional group at index " + position + ", got " + object);
		}
		return new ExpressionContext(evaluationContext, (List<Object>) object);
	}
}
