package io.datakernel.codegen.operation;

import static org.objectweb.asm.Opcodes.*;

public enum ArithmeticOperation {
	ADD(IADD, "+"),
	SUB(ISUB, "-"),
	MUL(IMUL, "*"),
	DIV(IDIV, "/"),
	REM(IREM, "%"),
	AND(IAND, "&"),
	OR(IOR, "|"),
	XOR(IXOR, "^"),
	SHL(ISHL, "<<"),
	SHR(ISHR, ">>"),
	USHR(IUSHR, ">>>");

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
		throw new IllegalArgumentException("Did not found operation for symbol " + symbol);
	}
}
