package io.datakernel.di.util;

public final class Constructors {
	private Constructors() {
		throw new AssertionError("nope.");
	}

//	public static void main(String[] args) {
//		for (int n = 0; n < 6; n++) {
//			System.out.println("\t@FunctionalInterface\n" +
//					"\tpublic interface Constructor" + n + "<" +
//					IntStream.range(1, n + 1).mapToObj(i -> "P" + i).collect(joining(", ")) +
//					(n != 0 ? ", " : "") +
//					"R> {\n" +
//					"\t\tR create(" +
//					IntStream.range(1, n + 1).mapToObj(i -> "P" + i + " p" + i).collect(joining(", ")) +
//					");\n" +
//					"\t}\n");
//		}
//	}

	@FunctionalInterface
	public interface Constructor0<R> {
		R create();
	}

	@FunctionalInterface
	public interface Constructor1<P1, R> {
		R create(P1 p1);
	}

	@FunctionalInterface
	public interface Constructor2<P1, P2, R> {
		R create(P1 p1, P2 p2);
	}

	@FunctionalInterface
	public interface Constructor3<P1, P2, P3, R> {
		R create(P1 p1, P2 p2, P3 p3);
	}

	@FunctionalInterface
	public interface Constructor4<P1, P2, P3, P4, R> {
		R create(P1 p1, P2 p2, P3 p3, P4 p4);
	}

	@FunctionalInterface
	public interface Constructor5<P1, P2, P3, P4, P5, R> {
		R create(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5);
	}

	@FunctionalInterface
	public interface Constructor6<P1, P2, P3, P4, P5, P6, R> {
		R create(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6);
	}
}
