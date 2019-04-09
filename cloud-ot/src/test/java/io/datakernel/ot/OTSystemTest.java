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

import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestAdd;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.ot.utils.TestOpState;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.ot.OTAlgorithms.merge;
import static io.datakernel.ot.OTAlgorithms.mergeAndUpdateHeads;
import static io.datakernel.ot.utils.Utils.*;
import static java.util.Arrays.asList;

@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument"})
@RunWith(DatakernelRunner.class)
public final class OTSystemTest {
	private static final OTSystem<TestOp> SYSTEM = createTestOp();

	@Test
	public void testTransform1() throws Exception {
		List<? extends TestOp> left = asList(add(2), add(1));
		List<? extends TestOp> right = asList(add(1), add(10), add(100));
		TransformResult<TestOp> result = SYSTEM.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);
	}

	@Test
	public void testTransform2() throws Exception {
		List<? extends TestOp> left = asList(add(2), set(2, 1), add(2), add(10));
		List<? extends TestOp> right = asList(set(0, -20), add(30), add(10));
		TransformResult<TestOp> result = SYSTEM.transform(left, right);
		System.out.println(result.left);
		System.out.println(result.right);
	}

	@Test
	public void testSimplify() {
		List<? extends TestOp> arg = asList(add(2), set(2, 1), add(2), add(10));
		List<TestOp> result = SYSTEM.squash(arg);
		System.out.println(result);
	}

	@Test
	public void testOtSource2() {
		OTRepositoryStub<String, TestOp> repository = OTRepositoryStub.create(asList("m", "x", "y", "m2"));
		repository.setGraph(g -> {
			g.add("*", "a1", add(1));
			g.add("a1", "a2", add(2));
			g.add("a2", "a3", add(4));
			g.add("*", "b1", add(10));
			g.add("b1", "b2", add(100));
		});

		TestOpState state = new TestOpState();
		OTNodeImpl<String, TestOp, OTCommit<String, TestOp>> node = OTNodeImpl.create(repository, SYSTEM);
		OTStateManager<String, TestOp> stateManager = OTStateManager.create(getCurrentEventloop(), SYSTEM, node, state);

		await(stateManager.checkout());

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();

		await(mergeAndUpdateHeads(repository, SYSTEM));

		System.out.println(await(repository.loadCommit("m")));
		System.out.println(stateManager);
		System.out.println();
		stateManager.add(new TestAdd(50));
		System.out.println(stateManager);

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();
		stateManager.add(new TestAdd(3));
		System.out.println(stateManager);

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();
		System.out.println(repository);
		System.out.println(stateManager);

		await(stateManager.sync());
		System.out.println(repository.loadCommit("x"));
		System.out.println(repository.loadCommit("y"));
		System.out.println(stateManager);
		System.out.println();
		System.out.println(repository);

		await(merge(repository, SYSTEM));
		System.out.println(stateManager);
		System.out.println();
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

		OTNodeImpl<String, TestOp, OTCommit<String, TestOp>> node = OTNodeImpl.create(otSource, SYSTEM);
		pullAndThenMergeAndPush(otSource, OTStateManager.create(getCurrentEventloop(), SYSTEM, node, new TestOpState()));
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

		OTNodeImpl<String, TestOp, OTCommit<String, TestOp>> node = OTNodeImpl.create(otSource, SYSTEM);
		pullAndThenMergeAndPush(otSource, OTStateManager.create(getCurrentEventloop(), SYSTEM, node, new TestOpState()));
	}

	private void pullAndThenMergeAndPush(OTRepository<String, TestOp> repository, OTStateManager<String, TestOp> stateManager) {
		await(stateManager.checkout());

		await(stateManager.sync());
		System.out.println(stateManager);
		System.out.println();

		await(merge(repository, SYSTEM));
		System.out.println(repository);
		System.out.println(stateManager);
	}
}
