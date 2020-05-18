package io.datakernel.dataflow.dsl;

import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.dsl.LambdaParser.OpType;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static io.datakernel.dataflow.dataset.Datasets.listConsumer;
import static java.util.stream.Collectors.joining;

public final class AST {

	public interface Statement {

		void evaluate(EvaluationContext context);
	}

	public interface Expression {

		<T> Dataset<T> evaluate(EvaluationContext context);
	}

	public interface LambdaExpression {

		//TODO some kind of evaluation for codegen here
	}

	public static class Query implements Statement {
		public final List<Statement> statements;

		public Query(List<Statement> statements) {
			this.statements = statements;
		}

		@Override
		public void evaluate(EvaluationContext context) {
			statements.forEach(statement -> statement.evaluate(context));
		}

		@Override
		public String toString() {
			return statements.stream().map(Statement::toString).collect(joining("\n"));
		}
	}

	private static String stringLiteral(String value) {
		return "\"" +
				value.replace("\\", "\\\\")
						.replace("\"", "\\\"")
						.replace("\n", "\\n")
						.replace("\r", "\\r")
						.replace("\t", "\\t")
						.replace("\b", "\\b")
						.replace("\f", "\\f")
				+ "\"";
	}

	public static final class Identifier implements Expression {
		public final String name;

		public Identifier(String name) {
			this.name = name;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> Dataset<T> evaluate(EvaluationContext context) {
			return (Dataset<T>) context.get(name);
		}

		@Override
		public String toString() {
			return name;
		}
	}

	public static final class Use implements Statement {
		public final String prefix;

		public Use(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public void evaluate(EvaluationContext context) {
			context.addPrefix(prefix);
		}

		@Override
		public String toString() {
			return "USE " + stringLiteral(prefix);
		}
	}

	public static final class Assignment implements Statement {
		public final Identifier identifier;
		public final Expression expression;

		public Assignment(Identifier identifier, Expression expression) {
			this.identifier = identifier;
			this.expression = expression;
		}

		@Override
		public void evaluate(EvaluationContext context) {
			context.put(identifier.name, expression.evaluate(context));
		}

		@Override
		public String toString() {
			return identifier + " = " + expression;
		}
	}

	public static final class Write implements Statement {
		public final Expression source;
		public final String destination;

		public Write(Expression source, String destination) {
			this.source = source;
			this.destination = destination;
		}

		@Override
		public void evaluate(EvaluationContext context) {
			listConsumer(source.evaluate(context), destination)
					.compileInto(context.getGraph());
		}

		@Override
		public String toString() {
			return "WRITE " + source + " INTO " + stringLiteral(destination);
		}
	}

	public static final class Repeat implements Statement {
		public final int times;
		public final Query query;

		public Repeat(int times, Query query) {
			this.times = times;
			this.query = query;
		}

		@Override
		public void evaluate(EvaluationContext context) {
			for (int i = 0; i < times; i++) {
				query.evaluate(context);
			}
		}

		@Override
		public String toString() {
			return "REPEAT " + times + " TIMES\n    " +
					query.statements.stream().map(Statement::toString).collect(joining("\n    ")) +
					"\nEND";
		}
	}

	public static final class UnaryOp implements LambdaExpression {
		public final OpType operator;
		public final LambdaExpression operand;

		public UnaryOp(OpType operator, LambdaExpression operand) {
			this.operator = operator;
			this.operand = operand;
		}

		@Override
		public String toString() {
			return operator.token + operand;
		}
	}

	public static final class BinaryOp implements LambdaExpression {
		public final OpType operator;
		public final LambdaExpression left, right;

		public BinaryOp(OpType operator, LambdaExpression left, LambdaExpression right) {
			this.operator = operator;
			this.left = left;
			this.right = right;
		}

		@Override
		public String toString() {
			return left + " " + operator.token + " " + right;
		}
	}

	public static final class FieldReference implements LambdaExpression {
		public static final FieldReference DOT = new FieldReference(null, "");

		@Nullable
		public final FieldReference parent;
		public final String field;

		public FieldReference(@Nullable FieldReference parent, String field) {
			this.parent = parent;
			this.field = field;
		}

		@Override
		public String toString() {
			return (parent != null ? parent : "") + "." + field;
		}
	}

	// this node can (and should) be flattened, but I keep it to store paren information for nice toString
	public static final class Parens implements LambdaExpression {
		public final LambdaExpression expr;

		public Parens(LambdaExpression expr) {
			this.expr = expr;
		}

		@Override
		public String toString() {
			return "(" + expr + ")";
		}
	}

	public static final class StringLiteral implements LambdaExpression {
		public final String value;

		public StringLiteral(String value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return stringLiteral(value);
		}
	}

	public static final class IntLiteral implements LambdaExpression {
		public final int value;

		public IntLiteral(int value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return "" + value;
		}
	}

	public static final class FloatLiteral implements LambdaExpression {
		public final float value;

		public FloatLiteral(float value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return "" + value;
		}
	}
}
