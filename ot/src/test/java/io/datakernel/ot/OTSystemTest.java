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
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.stream.processor.EventloopRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.util.Arrays.asList;

@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument", "WeakerAccess"})
@RunWith(DatakernelRunner.class)
public final class OTSystemTest {

	@Test
	@EventloopRule.DontRun
	public void testTransform1() throws Exception {
		OTSystem<TestOp> opSystem = createTestOp();
		List<? extends TestOp> left = asList(add(2), add(1));
		List<? extends TestOp> right = asList(add(1), add(10), add(100));
		TransformResult<TestOp> result = opSystem.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);
	}

	@Test
	@EventloopRule.DontRun
	public void testTransform2() throws Exception {
		OTSystem<TestOp> opSystem = createTestOp();
		List<? extends TestOp> left = asList(add(2), set(2, 1), add(2), add(10));
		List<? extends TestOp> right = asList(set(0, -20), add(30), add(10));
		TransformResult<TestOp> result = opSystem.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);
	}

	@Test
	@EventloopRule.DontRun
	public void testSimplify() {
		OTSystem<TestOp> opSystem = createTestOp();
		List<? extends TestOp> arg = asList(add(2), set(2, 1), add(2), add(10));
		List<TestOp> result = opSystem.squash(arg);
		System.out.println(result);
	}

	@Test
	public void testOtSource2() {
		OTSystem<TestOp> system = createTestOp();
		OTRepositoryStub<String, TestOp> repository = OTRepositoryStub.create(asList("m", "x", "y", "m2"));
		repository.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("a1", "a2", add(2));
			g.add("a2", "a3", add(4));
			g.add("*", "b1", add(10));
			g.add("b1", "b2", add(100));
		});

		TestOpState state = new TestOpState();
		OTAlgorithms<String, TestOp> algorithms = new OTAlgorithms<>(Eventloop.getCurrentEventloop(), system, repository);
		OTStateManager<String, TestOp> stateManager = new OTStateManager<>(Eventloop.getCurrentEventloop(), algorithms, state);

		stateManager.start()
				.thenCompose($ -> stateManager.pull())
				.whenComplete(assertComplete($ -> {
					System.out.println(stateManager);
					System.out.println();
				}))
				.thenCompose($ -> algorithms.mergeHeadsAndPush())
				.whenComplete(assertComplete($ -> {
					System.out.println(repository.loadCommit("m"));
					System.out.println(stateManager);
					System.out.println();
					stateManager.add(new TestAdd(50));
					System.out.println(stateManager);
				}))
				.thenCompose($ -> stateManager.commit())
				.whenComplete(assertComplete($ -> {
					System.out.println(stateManager);
					System.out.println();
					stateManager.add(new TestAdd(3));
					System.out.println(stateManager);
				}))
				.thenCompose($ -> stateManager.pull())
				.whenComplete(assertComplete($ -> {
					System.out.println(stateManager);
					System.out.println();
				}))
				.thenCompose($ -> stateManager.commit())
				.whenComplete(assertComplete($ -> {
					System.out.println(stateManager);
					System.out.println();
					System.out.println(repository);
					System.out.println(stateManager);
				}))
				.thenCompose($ -> stateManager.push())
				.whenComplete(assertComplete($ -> {
					System.out.println(repository.loadCommit("x"));
					System.out.println(repository.loadCommit("y"));
					System.out.println(stateManager);
					System.out.println();
					System.out.println(repository);
				}))
				.thenCompose($ -> algorithms.mergeHeadsAndPush())
				.whenComplete(assertComplete($ -> {
					System.out.println(stateManager);
					System.out.println();
				}));
	}

	@Test
	public void testOtSource3() {
		OTRepositoryStub<String, TestOp> otSource = OTRepositoryStub.create(asList("m"));
		otSource.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("a1", "a2", add(2));
			g.add("a2", "a3", add(4));
			g.add("a2", "b1", add(10));
		});

		OTAlgorithms<String, TestOp> algorithms = new OTAlgorithms<>(Eventloop.getCurrentEventloop(), createTestOp(), otSource);
		pullAndThenMergeAndPush(otSource, algorithms, new OTStateManager<>(Eventloop.getCurrentEventloop(), algorithms, new TestOpState()));
	}

	@Test
	public void testOtSource4() {
		OTRepositoryStub<String, TestOp> otSource = OTRepositoryStub.create(asList("m"));
		otSource.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("*", "b1", add(10));
			g.add("a1", "a2", add(10));
			g.add("b1", "a2", add(1));
			g.add("a1", "b2", add(10));
			g.add("b1", "b2", add(1));
		});

		OTAlgorithms<String, TestOp> algorithms = new OTAlgorithms<>(Eventloop.getCurrentEventloop(), createTestOp(), otSource);
		pullAndThenMergeAndPush(otSource, algorithms, new OTStateManager<>(Eventloop.getCurrentEventloop(), algorithms, new TestOpState()));
	}

	private void pullAndThenMergeAndPush(OTRepositoryStub<String, TestOp> otSource, OTAlgorithms<String, TestOp> algorithms, OTStateManager<String, TestOp> stateManager) {
		stateManager.start()
				.thenCompose($ -> stateManager.pull())
				.whenComplete(assertComplete($ -> {
					System.out.println(stateManager);
					System.out.println();
				}))
				.thenCompose($ -> algorithms.mergeHeadsAndPush())
				.whenComplete(assertComplete($ -> {
					System.out.println(otSource);
					System.out.println(stateManager);
				}));
	}
}
