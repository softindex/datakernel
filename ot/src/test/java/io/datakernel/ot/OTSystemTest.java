package io.datakernel.ot;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTSourceStub.TestSequence;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
				.withTransformFunction(TestAdd.class, TestAdd.class, (left, right) -> {
					check(left.prev == right.prev);
					return TransformResult.of(add(left.prev + left.delta, right.delta), add(right.prev + right.delta, left.delta));
				})
				.withTransformFunction(TestAdd.class, TestSet.class, (left, right) -> {
					check(left.prev == right.prev);
					return TransformResult.left(set(left.prev + left.delta, right.next));
				})
				.withTransformFunction(TestSet.class, TestSet.class, (left, right) -> {
					check(left.prev == right.prev);
					if (left.next > right.next)
						return TransformResult.left(set(left.next, right.next));
					if (left.next < right.next)
						return TransformResult.right(set(right.next, left.next));
					return TransformResult.empty();
				})
				.withSquashFunction(TestAdd.class, TestAdd.class, (op1, op2) -> {
					check(op1.prev + op1.delta == op2.prev);
					return add(op1.prev, op1.delta + op2.delta);
				})
				.withSquashFunction(TestSet.class, TestSet.class, (op1, op2) -> {
					check(op1.next == op2.prev);
					return set(op1.prev, op2.next);
				})
				.withSquashFunction(TestAdd.class, TestSet.class, (op1, op2) -> {
					check(op1.prev + op1.delta == op2.prev);
					return set(op1.prev, op2.next);
				})
				.withEmptyPredicate(TestAdd.class, add -> add.delta == 0)
				.withEmptyPredicate(TestSet.class, set -> set.prev == set.next)
				.withInvertFunction(TestAdd.class, op -> asList(add(op.prev + op.delta, -op.delta)))
				.withInvertFunction(TestSet.class, op -> asList(set(op.next, op.prev)))
				;
	}

	@Test
	public void testTransform1() throws Exception {
		OTSystem<TestOp> opSystem = create();
		List<? extends TestOp> left = asList(add(0, 2), add(2, 1));
		List<? extends TestOp> right = asList(add(0, 1), add(1, 10), add(11, 100));
		TransformResult<TestOp> result = opSystem.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);
	}

	@Test
	public void testTransform2() throws Exception {
		OTSystem<TestOp> opSystem = create();
		List<? extends TestOp> left = asList(add(0, 2), set(2, 1), add(1, 2), add(3, 10));
		List<? extends TestOp> right = asList(set(0, -20), add(-20, 30), add(10, 10));
		TransformResult<TestOp> result = opSystem.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);
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
	public void testOtSource2() throws Exception {
		final OTSystem<TestOp> system = OTSystemTest.create();
		Comparator<String> comparator = (o1, o2) -> reverse(o1).compareTo(reverse(o2));
		OTSourceStub<String, TestOp> otSource = OTSourceStub.create(TestSequence.of("m", "x", "y", "m2"), comparator);

		otSource.push(OTCommit.ofRoot("*"));
		otSource.add("*", "a1", asList(add(0, 1)));
		otSource.add("a1", "a2", asList(add(1, 2)));
		otSource.add("a2", "a3", asList(add(3, 4)));
		otSource.add("*", "b1", asList(add(0, 10)));
		otSource.add("b1", "b2", asList(add(10, 100)));

		Eventloop eventloop = Eventloop.create();
		TestOpState state = new TestOpState();
		OTStateManager<String, TestOp> stateManager = new OTStateManager<>(eventloop, system,
				otSource,
				comparator,
				state);

		stateManager.start(assertCompletion());
		eventloop.run();
		System.out.println(stateManager);
		System.out.println();

//		ResultCallbackFuture<Map<String, List<TestOp>>> future = ResultCallbackFuture.create();
//		OTUtils.doMerge(eventloop, system, otSource, comparator,
//				new HashSet<>(asList("*", "a1", "a2", "a3")),
//				new HashSet<>(Arrays.<String>asList()), "*",
//				future);
//		eventloop.run();
//		System.out.println(future.get());

		CompletableFuture<?> future;

		future = OTUtils.mergeHeadsAndPush(system, otSource, comparator).toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(otSource.loadCommit("m"));
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

		System.out.println(otSource);
		System.out.println(stateManager);
		future = stateManager.push().toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(otSource.loadCommit("x"));
		System.out.println(otSource.loadCommit("y"));
		System.out.println(stateManager);
		System.out.println();

		System.out.println(otSource);
		future = OTUtils.mergeHeadsAndPush(system, otSource, comparator).toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(stateManager);
		System.out.println();

	}

	@Test
	public void testOtSource3() throws Exception {
		final OTSystem<TestOp> system = OTSystemTest.create();
		Comparator<String> comparator = (o1, o2) -> reverse(o1).compareTo(reverse(o2));
		OTSourceStub<String, TestOp> otSource = OTSourceStub.create(TestSequence.of("m"), comparator);

		otSource.push(OTCommit.ofRoot("*"));
		otSource.add("*", "a1", asList(add(0, 1)));
		otSource.add("a1", "a2", asList(add(1, 2)));
		otSource.add("a2", "a3", asList(add(3, 4)));
		otSource.add("a2", "b1", asList(add(3, 10)));

		Eventloop eventloop = Eventloop.create();
		TestOpState state = new TestOpState();
		OTStateManager<String, TestOp> stateManager = new OTStateManager<>(eventloop, system,
				otSource,
				comparator,
				state);

		stateManager.start(assertCompletion());
		eventloop.run();
		System.out.println(stateManager);
		System.out.println();

		CompletableFuture<?> future;

		future = OTUtils.mergeHeadsAndPush(system, otSource, comparator).toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(otSource);
		System.out.println(stateManager);
	}

	@Test
	public void testOtSource4() throws Exception {
		final OTSystem<TestOp> system = OTSystemTest.create();
		Comparator<String> comparator = (o1, o2) -> reverse(o1).compareTo(reverse(o2));
		OTSourceStub<String, TestOp> otSource = OTSourceStub.create(TestSequence.of("m"), comparator);

		otSource.push(OTCommit.ofRoot("*"));
		otSource.add("*", "a1", asList(add(0, 1)));
		otSource.add("*", "b1", asList(add(0, 10)));
		otSource.add("a1", "a2", asList(add(1, 10)));
		otSource.add("b1", "a2", asList(add(10, 1)));
		otSource.add("a1", "b2", asList(add(1, 10)));
		otSource.add("b1", "b2", asList(add(10, 1)));

		Eventloop eventloop = Eventloop.create();
		TestOpState state = new TestOpState();
		OTStateManager<String, TestOp> stateManager = new OTStateManager<>(eventloop, system,
				otSource,
				comparator,
				state);

		stateManager.start(assertCompletion());
		eventloop.run();
		System.out.println(stateManager);
		System.out.println();

		CompletableFuture<?> future;

		future = OTUtils.mergeHeadsAndPush(system, otSource, comparator).toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(otSource);
		System.out.println(stateManager);
	}

}