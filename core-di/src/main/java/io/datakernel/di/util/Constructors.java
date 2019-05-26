package io.datakernel.di.util;

public final class Constructors {
	private Constructors() {
		throw new AssertionError("nope.");
	}

//	public static void main(String[] args) {
//		for (int n = 0; n <= 6; n++) {
//			System.out.printf("	@FunctionalInterface\n" +
//							"	public interface Constructor%d<%s%sR> extends Factory<R> {\n" +
//							"		R create(%s);\n\n" +
//							"%s" +
//							"		@Override\n" +
//							"		default R create(Object[] args) {\n" +
//							"			return create(%s);\n" +
//							"		}\n" +
//							"	}\n",
//					n,
//					IntStream.range(1, n + 1).mapToObj(i -> "P" + i).collect(joining(", ")),
//					n != 0 ? ", " : "",
//					IntStream.range(1, n + 1).mapToObj(i -> "P" + i + " p" + i).collect(joining(", ")),
//					n != 0 ? "		@SuppressWarnings(\"unchecked\")\n" : "",
//					IntStream.range(0, n).mapToObj(i -> "(P" + (i + 1) + ") args[" + i + "]").collect(joining(", ")));
//		}
//	}

	@FunctionalInterface
	public interface Factory<R> {
		R create(Object[] args);
	}

	@FunctionalInterface
	public interface Constructor0<R> extends Factory<R> {
		R create();

		@Override
		default R create(Object[] args) {
			return create();
		}
	}

	@FunctionalInterface
	public interface Constructor1<P1, R> extends Factory<R> {
		R create(P1 p1);

		@SuppressWarnings("unchecked")
		@Override
		default R create(Object[] args) {
			return create((P1) args[0]);
		}
	}

	@FunctionalInterface
	public interface Constructor2<P1, P2, R> extends Factory<R> {
		R create(P1 p1, P2 p2);

		@SuppressWarnings("unchecked")
		@Override
		default R create(Object[] args) {
			return create((P1) args[0], (P2) args[1]);
		}
	}

	@FunctionalInterface
	public interface Constructor3<P1, P2, P3, R> extends Factory<R> {
		R create(P1 p1, P2 p2, P3 p3);

		@SuppressWarnings("unchecked")
		@Override
		default R create(Object[] args) {
			return create((P1) args[0], (P2) args[1], (P3) args[2]);
		}
	}

	@FunctionalInterface
	public interface Constructor4<P1, P2, P3, P4, R> extends Factory<R> {
		R create(P1 p1, P2 p2, P3 p3, P4 p4);

		@SuppressWarnings("unchecked")
		@Override
		default R create(Object[] args) {
			return create((P1) args[0], (P2) args[1], (P3) args[2], (P4) args[3]);
		}
	}

	@FunctionalInterface
	public interface Constructor5<P1, P2, P3, P4, P5, R> extends Factory<R> {
		R create(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5);

		@SuppressWarnings("unchecked")
		@Override
		default R create(Object[] args) {
			return create((P1) args[0], (P2) args[1], (P3) args[2], (P4) args[3], (P5) args[4]);
		}
	}

	@FunctionalInterface
	public interface Constructor6<P1, P2, P3, P4, P5, P6, R> extends Factory<R> {
		R create(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6);

		@SuppressWarnings("unchecked")
		@Override
		default R create(Object[] args) {
			return create((P1) args[0], (P2) args[1], (P3) args[2], (P4) args[3], (P5) args[4], (P6) args[5]);
		}
	}
}
