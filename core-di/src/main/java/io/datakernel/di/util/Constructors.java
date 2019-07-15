package io.datakernel.di.util;

/**
 * These are just set of functional interfaces to be used by the DSL
 */
public final class Constructors {
	private Constructors() {}

	@FunctionalInterface
	public interface Constructor0<R> {
		R create();
	}

	@FunctionalInterface
	public interface Constructor1<P1, R> {
		R create(P1 arg1);
	}

	@FunctionalInterface
	public interface Constructor2<P1, P2, R> {
		R create(P1 arg1, P2 arg2);
	}

	@FunctionalInterface
	public interface Constructor3<P1, P2, P3, R> {
		R create(P1 arg1, P2 arg2, P3 arg3);
	}

	@FunctionalInterface
	public interface Constructor4<P1, P2, P3, P4, R> {
		R create(P1 arg1, P2 arg2, P3 arg3, P4 arg4);
	}

	@FunctionalInterface
	public interface Constructor5<P1, P2, P3, P4, P5, R> {
		R create(P1 arg1, P2 arg2, P3 arg3, P4 arg4, P5 arg5);
	}

	@FunctionalInterface
	public interface Constructor6<P1, P2, P3, P4, P5, P6, R> {
		R create(P1 arg1, P2 arg2, P3 arg3, P4 arg4, P5 arg5, P6 arg6);
	}

	@FunctionalInterface
	public interface ConstructorN<R> {
		R create(Object... args);
	}
}
