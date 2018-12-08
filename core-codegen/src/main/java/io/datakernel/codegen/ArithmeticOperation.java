package io.datakernel.codegen;

import static org.objectweb.asm.Opcodes.*;

public enum ArithmeticOperation {
	ADD(IADD, "+"), SUB(ISUB, "-"), MUL(IMUL, "*"), DIV(IDIV, "/"), REM(IREM, "%");

	public final int opCode;
	public final String symbol;

	ArithmeticOperation(int opCode, String symbol) {
		this.opCode = opCode;
		this.symbol = symbol;
	}

	public static ArithmeticOperation operation(String symbol) {
		for (ArithmeticOperation operation : ArithmeticOperation.values()) {
			if (operation.symbol.equals(symbol)) {
				return operation;
			}
		}
		throw new IllegalArgumentException();
	}
}
