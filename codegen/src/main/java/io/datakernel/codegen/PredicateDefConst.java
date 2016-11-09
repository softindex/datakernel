package io.datakernel.codegen;

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

public class PredicateDefConst implements PredicateDef {
	private final boolean value;

	public PredicateDefConst(boolean value) {
		this.value = value;
	}

	@Override
	public Type type(Context ctx) {
		return Type.BOOLEAN_TYPE;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		g.push(value);
		return Type.BOOLEAN_TYPE;
	}
}
