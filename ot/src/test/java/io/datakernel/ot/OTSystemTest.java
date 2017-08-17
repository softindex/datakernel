package io.datakernel.ot;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTSystemImpl.SquashFunction;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.Maps.transformValues;
import static io.datakernel.async.AsyncCallbacks.assertCompletion;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Arrays.asList;

@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument", "WeakerAccess"})
public class OTSystemTest {
	public interface TestOp {
		int apply(int prev);
	}

	public static class TestAdd implements TestOp {
		final int prev;
		final int delta;

		private TestAdd(int prev, int delta) {
			this.prev = prev;
			this.delta = delta;
		}

		public TestAdd inverse() {
			int delta1 = -delta;
			return new TestAdd(prev + delta, delta1);
		}

		@Override
		public String toString() {
			return prev + "+=" + delta;
		}

		@Override
		public int apply(int prev) {
			checkState(prev == this.prev);
			return prev + delta;
		}
	}

	public static class TestSet implements TestOp {
		final int prev;
		final int next;

		public TestSet(int prev, int next) {
			this.prev = prev;
			this.next = next;
		}

		public TestSet inverse() {
			return new TestSet(next, prev);
		}

		@Override
		public String toString() {
			return prev + ":=" + next;
		}

		@Override
		public int apply(int prev) {
			checkState(prev == this.prev);
			return next;
		}
	}

	public static TestAdd add(int prev, int delta) {
		return new TestAdd(prev, delta);
	}

	public static TestSet set(int prev, int next) {
		return new TestSet(prev, next);
	}

	public static class TestOpState implements OTState<TestOp> {
		int value;
		TestOp last;

		@Override
		public void init() {
			value = 0;
			last = null;
		}

		@Override
		public void apply(TestOp testOp) {
			value = testOp.apply(value);
			last = testOp;
		}

		@Override
		public String toString() {
			return "" + value;
		}
	}

	@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
	public static OTSystem<TestOp> create() {
		return OTSystemImpl.<TestOp>create()
				.withTransformFunction(TestAdd.class, TestAdd.class, new OTSystemImpl.TransformFunction<TestOp, TestAdd, TestAdd>() {
					@Override
					public DiffPair<? extends TestOp> transform(TestAdd left, TestAdd right) {
						check(left.prev == right.prev);
						return DiffPair.of(add(left.prev + left.delta, right.delta), add(right.prev + right.delta, left.delta));
					}
				})
				.withTransformFunction(TestAdd.class, TestSet.class, new OTSystemImpl.TransformFunction<TestOp, TestAdd, TestSet>() {
					@Override
					public DiffPair<? extends TestOp> transform(TestAdd left, TestSet right) {
						check(left.prev == right.prev);
						return DiffPair.left(set(left.prev + left.delta, right.next));
					}
				})
				.withTransformFunction(TestSet.class, TestSet.class, new OTSystemImpl.TransformFunction<TestOp, TestSet, TestSet>() {
					@Override
					public DiffPair<? extends TestOp> transform(TestSet left, TestSet right) {
						check(left.prev == right.prev);
						if (left.next > right.next)
							return DiffPair.left(set(left.next, right.next));
						if (left.next < right.next)
							return DiffPair.right(set(right.next, left.next));
						return DiffPair.empty();
					}
				})
				.withSquashFunction(TestAdd.class, TestAdd.class, new SquashFunction<TestOp, TestAdd, TestAdd>() {
					@Override
					public TestOp trySquash(TestAdd op1, TestAdd op2) {
						check(op1.prev + op1.delta == op2.prev);
						return add(op1.prev, op1.delta + op2.delta);
					}
				})
				.withSquashFunction(TestSet.class, TestSet.class, new SquashFunction<TestOp, TestSet, TestSet>() {
					@Override
					public TestOp trySquash(TestSet op1, TestSet op2) {
						check(op1.next == op2.prev);
						return set(op1.prev, op2.next);
					}
				})
				.withSquashFunction(TestAdd.class, TestSet.class, new SquashFunction<TestOp, TestAdd, TestSet>() {
					@Override
					public TestSet trySquash(TestAdd op1, TestSet op2) {
						check(op1.prev + op1.delta == op2.prev);
						return set(op1.prev, op2.next);
					}
				})
				.withEmptyPredicate(TestAdd.class, new OTSystemImpl.EmptyPredicate<TestAdd>() {
					@Override
					public boolean isEmpty(TestAdd add) {
						return add.delta == 0;
					}
				})
				.withEmptyPredicate(TestSet.class, new OTSystemImpl.EmptyPredicate<TestSet>() {
					@Override
					public boolean isEmpty(TestSet set) {
						return set.prev == set.next;
					}
				})
				.withInvertFunction(TestAdd.class, new OTSystemImpl.InvertFunction<TestAdd>() {
					@Override
					public List<? extends TestAdd> invert(TestAdd op) {
						return asList(add(op.prev + op.delta, -op.delta));
					}
				})
				.withInvertFunction(TestSet.class, new OTSystemImpl.InvertFunction<TestSet>() {
					@Override
					public List<? extends TestSet> invert(TestSet op) {
						return asList(set(op.next, op.prev));
					}
				})
				;
	}

	@Test
	public void testTransform1() throws Exception {
		OTSystem<TestOp> opSystem = create();
		List<? extends TestOp> left = asList(add(0, 2), add(2, 1));
		List<? extends TestOp> right = asList(add(0, 1), add(1, 10), add(11, 100));
		DiffPair<TestOp> result = opSystem.transform(DiffPair.of(left, right));
		System.out.println(result.left);
		System.out.println(result.right);
	}

	@Test
	public void testTransform2() throws Exception {
		OTSystem<TestOp> opSystem = create();
		List<? extends TestOp> left = asList(add(0, 2), set(2, 1), add(1, 2), add(3, 10));
		List<? extends TestOp> right = asList(set(0, -20), add(-20, 30), add(10, 10));
		DiffPair<TestOp> result = opSystem.transform(DiffPair.of(left, right));
		System.out.println(result.left);
		System.out.println(result.right);
	}

	@Test
	public void testMultiTransform1() throws Exception {
		OTSystem<TestOp> opSystem = create();
		List<? extends TestOp> a = asList(add(0, 2), add(2, 10));
		List<? extends TestOp> b = asList(add(0, -10), set(-10, -20), add(-20, 30));
//		List<? extends TestOp> b = asList(add(0, 30), add(30, 10));
		List<? extends TestOp> c = asList(add(0, 5));
		List[] result = opSystem.transform(new List[]{a, b, c});
		System.out.println(result[0]);
		System.out.println(result[1]);
		System.out.println(result[2]);
	}

	@Test
	public void testSimplify() throws Exception {
		OTSystem<TestOp> opSystem = create();
		List<? extends TestOp> arg = asList(add(0, 2), set(2, 1), add(1, 2), add(3, 10));
		List<TestOp> result = opSystem.squash(arg);
		System.out.println(result);
	}

	private static String reverse(String string) {
		String result = "";
		for (int i = 0; i < string.length(); i++) {
			result += string.charAt(string.length() - i - 1);
		}
		return result;
	}

	@Test
	public void testGraph1() throws Exception {
		final OTSystem<TestOp> system = OTSystemTest.create();
		OTGraph<String, TestOp> graph = new OTGraph<String, TestOp>(system, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return -o1.compareTo(o2);
			}
		});
		graph.add("*", "a1", asList(add(0, 1)));
		graph.add("*", "a2", asList(add(0, 2)));
		graph.add("a1", "b1", null);
		graph.add("a2", "b1", asList(add(2, 1)));
		graph.add("a1", "b2", null);
		graph.add("a2", "b2", asList(add(2, 1)));
		Map<String, List<TestOp>> merged = graph.merge(ImmutableSet.of("a2", "*"));
		System.out.println(merged);
		System.out.println(transformValues(merged, new Function<List<TestOp>, List<TestOp>>() {
			@Override
			public List<TestOp> apply(List<TestOp> input) {
				return system.squash(input);
			}
		}));
	}

	@Test
	public void testGraph2() throws Exception {
		final OTSystem<TestOp> system = OTSystemTest.create();
		OTGraph<String, TestOp> graph = new OTGraph<>(system, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return reverse(o1).compareTo(reverse(o2));
			}
		});
		graph.add("*", "a1", asList(add(0, 1)));
		graph.add("a1", "a2", asList(add(1, 2)));
		graph.add("a2", "a3", asList(add(3, 4)));
		graph.add("*", "b1", asList(add(0, 10)));
		graph.add("b1", "b2", asList(add(10, 100)));
		Map<String, List<TestOp>> merged = graph.merge(ImmutableSet.of("a3", "b2"));
		System.out.println(merged);
		System.out.println(transformValues(merged, new Function<List<TestOp>, List<TestOp>>() {
			@Override
			public List<TestOp> apply(List<TestOp> input) {
				return system.squash(input);
			}
		}));
	}

	@Test
	public void testGraph2m() throws Exception {
		final OTSystem<TestOp> system = OTSystemTest.create();
		OTGraph<String, TestOp> graph = new OTGraph<>(system, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return reverse(o1).compareTo(reverse(o2));
			}
		});
		graph.add("*", "a1", asList(add(0, 1)));
		graph.add("*", "b1", asList(add(0, 10)));

		graph.add("a1", "m", asList(add(1, 10)));
		graph.add("b1", "m", asList(add(10, 1)));

		Map<String, List<TestOp>> merged = graph.merge(ImmutableSet.of("a1", "m"));
		System.out.println(merged);
		System.out.println(transformValues(merged, new Function<List<TestOp>, List<TestOp>>() {
			@Override
			public List<TestOp> apply(List<TestOp> input) {
				return system.squash(input);
			}
		}));
	}

	@Test
	public void testGraph3() throws Exception {
		final OTSystem<TestOp> system = OTSystemTest.create();
		OTGraph<String, TestOp> graph = new OTGraph<>(system, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return reverse(o1).compareTo(reverse(o2));
			}
		});
		graph.add("*", "a1", asList(add(0, 1)));
		graph.add("a1", "a2", asList(add(1, 2)));
		graph.add("*", "b1", Collections.<TestOp>emptyList());
		graph.add("b1", "b2", Collections.<TestOp>emptyList());
		Map<String, List<TestOp>> merged = graph.merge(ImmutableSet.of("*", "a2"));
		System.out.println(merged);
		System.out.println(transformValues(merged, new Function<List<TestOp>, List<TestOp>>() {
			@Override
			public List<TestOp> apply(List<TestOp> input) {
				return system.squash(input);
			}
		}));
	}

	@Test
	public void testOtSource2() throws Exception {
		final OTSystem<TestOp> system = OTSystemTest.create();
		Comparator<String> comparator = new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return reverse(o1).compareTo(reverse(o2));
			}
		};
		OTSourceStub<String, TestOp> otSourceStub = OTSourceStub.create(OTSourceStub.ProvidedSequence.of("m", "x", "y", "m2"), comparator);

		otSourceStub.push(OTCommit.<String, TestOp>ofRoot("*"));
		otSourceStub.add("*", "a1", asList(add(0, 1)));
		otSourceStub.add("a1", "a2", asList(add(1, 2)));
		otSourceStub.add("a2", "a3", asList(add(3, 4)));
		otSourceStub.add("*", "b1", asList(add(0, 10)));
		otSourceStub.add("b1", "b2", asList(add(10, 100)));

		Eventloop eventloop = Eventloop.create();
		TestOpState state = new TestOpState();
		OTStateManager<String, TestOp> stateManager = new OTStateManager<>(eventloop, system,
				otSourceStub,
				comparator,
				state);

		stateManager.start(assertCompletion());
		eventloop.run();
		System.out.println(stateManager);
		System.out.println();

//		ResultCallbackFuture<Map<String, List<TestOp>>> future = ResultCallbackFuture.create();
//		OTUtils.doMerge(eventloop, system, otSourceStub, comparator,
//				new HashSet<>(asList("*", "a1", "a2", "a3")),
//				new HashSet<>(Arrays.<String>asList()), "*",
//				future);
//		eventloop.run();
//		System.out.println(future.get());

		CompletableFuture<?> future;

		future = OTUtils.mergeHeadsAndPush(system, otSourceStub, comparator).toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(otSourceStub.loadCommit("m"));
		System.out.println(stateManager);
		System.out.println();

		stateManager.add(new TestAdd(110, 50));
		System.out.println(stateManager);
		future = stateManager.commit().toCompletableFuture();
		eventloop.run();
		future.get();
		future.get();
		System.out.println(stateManager);
		System.out.println();

		stateManager.add(new TestAdd(160, 3));
		System.out.println(stateManager);
		future = stateManager.pull().toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(stateManager);
		System.out.println();

		future = stateManager.commit().toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(stateManager);
		System.out.println();

		System.out.println(otSourceStub);
		System.out.println(stateManager);
		future = stateManager.push().toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(otSourceStub.loadCommit("x"));
		System.out.println(otSourceStub.loadCommit("y"));
		System.out.println(stateManager);
		System.out.println();

		System.out.println(otSourceStub);
		future = OTUtils.mergeHeadsAndPush(system, otSourceStub, comparator).toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(stateManager);
		System.out.println();

	}

}