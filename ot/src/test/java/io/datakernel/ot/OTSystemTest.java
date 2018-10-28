/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.ot;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestAdd;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.stream.processor.ActivePromisesRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.util.Arrays.asList;

@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument", "WeakerAccess"})
public class OTSystemTest {
	@Rule
	public ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Test
	public void testTransform1() throws Exception {
		OTSystem<TestOp> opSystem = createTestOp();
		List<? extends TestOp> left = asList(add(2), add(1));
		List<? extends TestOp> right = asList(add(1), add(10), add(100));
		TransformResult<TestOp> result = opSystem.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);
	}

	@Test
	public void testTransform2() throws Exception {
		OTSystem<TestOp> opSystem = createTestOp();
		List<? extends TestOp> left = asList(add(2), set(2, 1), add(2), add(10));
		List<? extends TestOp> right = asList(set(0, -20), add(30), add(10));
		TransformResult<TestOp> result = opSystem.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);
	}

	@Test
	public void testSimplify() throws Exception {
		OTSystem<TestOp> opSystem = createTestOp();
		List<? extends TestOp> arg = asList(add(2), set(2, 1), add(2), add(10));
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
		OTSystem<TestOp> system = createTestOp();
		OTRepositoryStub<String, TestOp> repository = OTRepositoryStub.create(asList("m", "x", "y", "m2"));
		repository.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("a1", "a2", add(2));
			g.add("a2", "a3", add(4));
			g.add("*", "b1", add(10));
			g.add("b1", "b2", add(100));
		});

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		TestOpState state = new TestOpState();
		OTAlgorithms<String, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, repository);
		OTStateManager<String, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, state);

		stateManager.start().thenCompose($ -> stateManager.pull()).whenComplete(assertComplete());
		eventloop.run();
		System.out.println(stateManager);
		System.out.println();

		//		ResultCallbackFuture<Map<String, List<TestOp>>> future = ResultCallbackFuture.create();
//		OTUtils.doMerge(eventloop, system, repository, comparator,
//				new HashSet<>(asList("*", "a1", "a2", "a3")),
//				new HashSet<>(Arrays.<String>asList()), "*",
//				future);
//		eventloop.run();
//		System.out.println(future.get());

		CompletableFuture<?> future;

		future = algorithms.mergeHeadsAndPush().toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(repository.loadCommit("m"));
		System.out.println(stateManager);
		System.out.println();

		stateManager.add(new TestAdd(50));
		System.out.println(stateManager);
		future = stateManager.commit().toCompletableFuture();
		eventloop.run();
		future.get();
		future.get();
		System.out.println(stateManager);
		System.out.println();

		stateManager.add(new TestAdd(3));
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

		System.out.println(repository);
		System.out.println(stateManager);
		future = stateManager.push().toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(repository.loadCommit("x"));
		System.out.println(repository.loadCommit("y"));
		System.out.println(stateManager);
		System.out.println();

		System.out.println(repository);
		future = algorithms.mergeHeadsAndPush().toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(stateManager);
		System.out.println();

	}

	@Test
	public void testOtSource3() throws Exception {
		OTSystem<TestOp> system = createTestOp();

		OTRepositoryStub<String, TestOp> otSource = OTRepositoryStub.create(asList("m"));
		otSource.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("a1", "a2", add(2));
			g.add("a2", "a3", add(4));
			g.add("a2", "b1", add(10));
		});

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		TestOpState state = new TestOpState();
		OTAlgorithms<String, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, otSource);
		OTStateManager<String, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, state);

		stateManager.start().thenCompose($ -> stateManager.pull()).whenComplete(assertComplete());
		eventloop.run();
		System.out.println(stateManager);
		System.out.println();

		CompletableFuture<?> future;

		future = algorithms.mergeHeadsAndPush().toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(otSource);
		System.out.println(stateManager);
	}

	@Test
	public void testOtSource4() throws Exception {
		OTSystem<TestOp> system = createTestOp();
		OTRepositoryStub<String, TestOp> otSource = OTRepositoryStub.create(asList("m"));
		otSource.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("*", "b1", add(10));
			g.add("a1", "a2", add(10));
			g.add("b1", "a2", add(1));
			g.add("a1", "b2", add(10));
			g.add("b1", "b2", add(1));
		});

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		TestOpState state = new TestOpState();
		OTAlgorithms<String, TestOp> algorithms = new OTAlgorithms<>(eventloop, system, otSource);
		OTStateManager<String, TestOp> stateManager = new OTStateManager<>(eventloop, algorithms, state);

		stateManager.start().thenCompose($ -> stateManager.pull()).whenComplete(assertComplete());
		eventloop.run();
		System.out.println(stateManager);
		System.out.println();

		CompletableFuture<?> future;

		future = algorithms.mergeHeadsAndPush().toCompletableFuture();
		eventloop.run();
		future.get();
		System.out.println(otSource);
		System.out.println(stateManager);
	}

}
