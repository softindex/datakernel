package io.datakernel.dataflow.dsl;

import io.datakernel.dataflow.dsl.AST.*;
import org.jparsec.*;

import java.util.Arrays;
import java.util.stream.Stream;

import static io.datakernel.dataflow.dsl.LambdaParser.Arity.BINARY;
import static io.datakernel.dataflow.dsl.LambdaParser.Arity.UNARY;

public final class LambdaParser {
	public static Parser<LambdaExpression> create(DslParser parser) {
		Parser.Reference<LambdaExpression> lambdaExprRef = Parser.newReference();

		Parser<Token> dot = parser.getTokenParser(".");

		Parser<Parens> parensParser = lambdaExprRef.lazy()
				.between(parser.getTokenParser("("), parser.getTokenParser(")"))
				.map(Parens::new);

		Parser<LambdaExpression> simpleExpr = Parsers.or(
				parensParser,

				dot.next(Terminals.identifier())
						.many1()
						.map(strs -> {
							FieldReference reference = null;
							// java has no proper foldLeft (at least in jdk8)
							for (String str : strs) {
								reference = new FieldReference(reference, str);
							}
							return reference;
						})
						.or(dot.retn(FieldReference.DOT)),

				DslParser.INT_LITERAL.map(IntLiteral::new),
				DslParser.FLOAT_LITERAL.map(FloatLiteral::new),
				DslParser.STRING_LITERAL.map(StringLiteral::new)
		);

		OperatorTable<LambdaExpression> operatorTable = new OperatorTable<>();
		for (OpType op : OpType.values()) {
			Parser<Token> tokenParser = parser.getTokenParser(op.token);
			if (op.arity == BINARY) {
				operatorTable.infixl(tokenParser.retn((left, right) -> new BinaryOp(op, left, right)), op.precedence);
			} else if (op.arity == UNARY) {
				operatorTable.prefix(tokenParser.retn(expr -> new UnaryOp(op, expr)), op.precedence);
			}
		}
		lambdaExprRef.set(operatorTable.build(simpleExpr));
		return parensParser.cast();
	}

	public static Stream<String> getOperatorTokens() {
		return Arrays.stream(OpType.values()).map(t -> t.token);
	}

	public enum Arity {
		UNARY, BINARY
	}

	// precedence is the same as in Java
	public enum OpType {
		OR(BINARY, "||", 10),
		AND(BINARY, "&&", 20),
		EQ(BINARY, "==", 30), NE(BINARY, "!=", 30),
		LE(BINARY, "<=", 40), GE(BINARY, ">=", 40), LT(BINARY, "<", 40), GT(BINARY, ">", 40),
		ADD(BINARY, "+", 50), SUB(BINARY, "-", 50),
		MUL(BINARY, "*", 60), DIV(BINARY, "/", 60), MOD(BINARY, "%", 60),

		NOT(UNARY, "!", 70),
		MINUS(UNARY, "-", 70),
		;

		public final Arity arity;
		public final String token;
		public final int precedence;

		OpType(Arity arity, String token, int precedence) {
			this.arity = arity;
			this.token = token;
			this.precedence = precedence;
		}
	}
}
