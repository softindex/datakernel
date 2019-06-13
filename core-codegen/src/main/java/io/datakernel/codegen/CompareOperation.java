package io.datakernel.codegen;

import org.objectweb.asm.commons.GeneratorAdapter;

public enum CompareOperation {
	EQ(GeneratorAdapter.EQ, "=="),
	NE(GeneratorAdapter.NE, "!="),
	LT(GeneratorAdapter.LT, "<"),
	GT(GeneratorAdapter.GT, ">"),
	LE(GeneratorAdapter.LE, "<="),
	GE(GeneratorAdapter.GE, ">=");

	public final int opCode;
	public final String symbol;

	CompareOperation(int opCode, String symbol) {
		this.opCode = opCode;
		this.symbol = symbol;
	}

	public static CompareOperation operation(String symbol) {
		for (CompareOperation operation : values()) {
			if (operation.symbol.equals(symbol)) {
				return operation;
			}
		}
		throw new IllegalArgumentException("Did not found operation for symbol " + symbol);
	}
}
