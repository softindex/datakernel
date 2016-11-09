package io.datakernel.codegen;

import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

public class PredicateDefNot implements PredicateDef {
	private final PredicateDef predicateDef;

	public PredicateDefNot(PredicateDef predicateDef) {
		this.predicateDef = predicateDef;
	}

	@Override
	public Type type(Context ctx) {
		return Type.BOOLEAN_TYPE;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label labelFalse = g.newLabel();
		Label labelExit = g.newLabel();
		predicateDef.load(ctx);
		g.ifZCmp(GeneratorAdapter.EQ, labelFalse);
		g.push(false);
		g.goTo(labelExit);
		g.visitLabel(labelFalse);
		g.push(true);
		g.visitLabel(labelExit);
		return Type.BOOLEAN_TYPE;
	}
}
