package io.global.ot.http;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.parse.ParseException;
import io.datakernel.http.StubHttpClient;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTUplink.FetchData;
import io.datakernel.ot.OTUplinkImpl;
import io.datakernel.ot.utils.OTRepositoryStub;
import io.datakernel.ot.utils.TestOp;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.global.common.SimKey;
import io.global.ot.api.CommitId;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.promise.TestUtils.await;
import static io.global.common.CryptoUtils.sha256;
import static io.global.ot.util.TestUtils.TEST_OP_CODEC;
import static io.global.ot.util.TestUtils.getCommitId;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class OTUplinkHttpClientTest {
	private static final StructuredCodec<TestOp> diffCodec = TEST_OP_CODEC;
	private static final SimKey SIM_KEY = SimKey.generate();
	private static final OTSystem<TestOp> OT_SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private OTUplinkHttpClient<CommitId, TestOp> client;
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
			g.add(getCommitId(1), getCommitId(2), add(1));
			g.add(getCommitId(2), getCommitId(3), set(-12, 34));
			g.add(getCommitId(3), getCommitId(4), add(-12));
			g.add(getCommitId(4), getCommitId(5), set(4, 5));
		});
		OTUplinkImpl<CommitId, TestOp, OTCommit<CommitId, TestOp>> uplink = OTUplinkImpl.create(repository, OT_SYSTEM);
		StubHttpClient stubClient = StubHttpClient.of(OTUplinkServlet.forGlobalNode(uplink, diffCodec, adapter));
		client = OTUplinkHttpClient.forGlobalNode(stubClient, "http://localhost/", diffCodec);
	}

	@Test
	public void testFetch() {
		FetchData<CommitId, TestOp> fetchData = await(client.fetch(getCommitId(3)));
		assertEquals(getCommitId(5), fetchData.getCommitId());
		assertEquals(OT_SYSTEM.squash(asList(add(-12), set(4, 5))), fetchData.getDiffs());
	}

	@Test
	public void testCheckout() {
		FetchData<CommitId, TestOp> checkoutData = await(client.checkout());
		assertEquals(getCommitId(5), checkoutData.getCommitId());
		assertEquals(OT_SYSTEM.squash(asList(add(1), set(-12, 34), add(-12), set(4, 5))), checkoutData.getDiffs());
	}

	@Test
	public void testCreateCommit() throws ParseException {
		CommitId parent = getCommitId(5);
		List<TestOp> diffs = asList(add(100), set(90, -34));

		byte[] rawData = await(client.createProtoCommit(parent, diffs, parent.getLevel()));
		OTCommit<CommitId, TestOp> otCommit = adapter.parseRawBytes(rawData);
		assertEquals(map(parent, diffs), otCommit.getParents());
		assertEquals(parent.getLevel() + 1, otCommit.getLevel());
		assertEquals(CommitId.of(parent.getLevel() + 1, sha256(rawData)), otCommit.getId());
	}

	@Test
	public void testPush() throws ParseException {
		CommitId parent = getCommitId(5);
		List<TestOp> diffs = asList(add(100), set(90, -34));

		byte[] commit = await(client.createProtoCommit(parent, diffs, parent.getLevel()));
		FetchData<CommitId, TestOp> fetchData = await(client.push(commit));
		CommitId commitId = fetchData.getCommitId();
		assertEquals(adapter.parseRawBytes(commit).getId(), commitId);

		Set<CommitId> heads = await(repository.getHeads());
		assertEquals(singleton(commitId), heads);
		OTCommit<CommitId, TestOp> headCommit = await(repository.loadCommit(commitId));

		assertEquals(map(parent, diffs), headCommit.getParents());
		assertEquals(parent.getLevel() + 1, headCommit.getLevel());
	}

}
