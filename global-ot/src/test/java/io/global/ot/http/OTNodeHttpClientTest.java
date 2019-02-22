package io.global.ot.http;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTNode.FetchData;
import io.datakernel.ot.OTNodeImpl;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.SimKey;
import io.global.ot.api.CommitId;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Set;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.common.CryptoUtils.sha256;
import static io.global.ot.util.TestUtils.TEST_OP_CODEC;
import static io.global.ot.util.TestUtils.getCommitId;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
@RunWith(DatakernelRunner.class)
public class OTNodeHttpClientTest {
	private static final StructuredCodec<TestOp> diffCodec = TEST_OP_CODEC;
	private static final SimKey SIM_KEY = SimKey.generate();
	private static final OTSystem<TestOp> OT_SYSTEM = createTestOp();

	private OTNodeHttpClient<CommitId, TestOp> client;
	private OTRepositoryStub<CommitId, TestOp> repository;
	private OTRepositoryAdapter<TestOp> adapter;

	@Before
	public void setUp() {
		OTDriver driver = new OTDriver(null, SIM_KEY);
		MyRepositoryId<TestOp> myRepositoryId = new MyRepositoryId<>(null, null, diffCodec);
		adapter = new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
		repository = OTRepositoryStub.create();
		repository.setCommitFactory(adapter);
		repository.setGraph(g -> {
			g.add(getCommitId(0), getCommitId(1), add(1));
			g.add(getCommitId(1), getCommitId(2), set(-12, 34));
			g.add(getCommitId(2), getCommitId(3), add(-12));
			g.add(getCommitId(3), getCommitId(4), set(4, 5));
		});
		OTAlgorithms<CommitId, TestOp> algorithms = OTAlgorithms.create(getCurrentEventloop(), OT_SYSTEM, repository);
		OTNodeImpl<CommitId, TestOp, OTCommit<CommitId, TestOp>> node = OTNodeImpl.create(algorithms);
		OTNodeServlet<CommitId, TestOp, OTCommit<CommitId, TestOp>> servlet = OTNodeServlet.forGlobalNode(node, diffCodec, adapter);
		client = OTNodeHttpClient.forGlobalNode(servlet::serve, "http://localhost/", diffCodec);
	}

	@Test
	public void testFetch() {
		FetchData<CommitId, TestOp> fetchData = await(client.fetch(getCommitId(2)));
		assertEquals(getCommitId(4), fetchData.getCommitId());
		assertEquals(5, fetchData.getLevel());
		assertEquals(OT_SYSTEM.squash(asList(add(-12), set(4, 5))), fetchData.getDiffs());
	}

	@Test
	public void testCheckout() {
		FetchData<CommitId, TestOp> checkoutData = await(client.checkout());
		assertEquals(getCommitId(4), checkoutData.getCommitId());
		assertEquals(5, checkoutData.getLevel());
		assertEquals(OT_SYSTEM.squash(asList(add(1), set(-12, 34), add(-12), set(4, 5))), checkoutData.getDiffs());
	}

	@Test
	public void testCreateCommit() throws ParseException {
		CommitId parent = getCommitId(4);
		List<TestOp> diffs = asList(add(100), set(90, -34));
		long level = 6;

		byte[] rawData = await(client.createCommit(parent, diffs, level));
		OTCommit<CommitId, TestOp> otCommit = adapter.parseRawBytes(rawData);
		assertEquals(map(parent, diffs), otCommit.getParents());
		assertEquals(level, otCommit.getLevel());
		assertEquals(CommitId.ofBytes(sha256(rawData)), otCommit.getId());
	}

	@Test
	public void testPush() throws ParseException {
		CommitId parent = getCommitId(4);
		List<TestOp> diffs = asList(add(100), set(90, -34));
		long level = 6;

		byte[] commit = await(client.createCommit(parent, diffs, level));
		CommitId commitId = await(client.push(commit));
		assertEquals(adapter.parseRawBytes(commit).getId(), commitId);

		Set<CommitId> heads = await(repository.getHeads());
		assertEquals(singleton(commitId), heads);
		OTCommit<CommitId, TestOp> headCommit = await(repository.loadCommit(commitId));

		assertEquals(map(parent, diffs), headCommit.getParents());
		assertEquals(level, headCommit.getLevel());
	}

}
