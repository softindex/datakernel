package io.datakernel.codegen;

import static org.objectweb.asm.Opcodes.*;

public enum BitOperation {
	SHL(ISHL, "<<"), SHR(ISHR, ">>"), USHR(IUSHR, ">>>"), AND(IAND, "&"), OR(IOR, "|"), XOR(IXOR, "^");

	public final int opCode;
	public final String symbol;

	BitOperation(int opCode, String symbol) {
		this.opCode = opCode;
		this.symbol = symbol;
	}

	public static BitOperation operation(String symbol) {
		for (BitOperation operation : BitOperation.values()) {
			if (operation.symbol.equals(symbol)) {
				return operation;
			}
		}
		throw new IllegalArgumentException();
	}
}
