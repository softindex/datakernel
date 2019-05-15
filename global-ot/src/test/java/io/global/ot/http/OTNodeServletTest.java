package io.global.ot.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.UrlBuilder;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTNode;
import io.datakernel.ot.OTNode.FetchData;
import io.datakernel.ot.OTNodeImpl;
import io.datakernel.ot.OTSystem;
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

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpRequest.get;
import static io.datakernel.http.HttpRequest.post;
import static io.datakernel.ot.utils.Utils.*;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.common.CryptoUtils.sha256;
import static io.global.ot.api.OTNodeCommand.*;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static io.global.ot.util.TestUtils.TEST_OP_CODEC;
import static io.global.ot.util.TestUtils.getCommitId;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.Assert.assertEquals;

public class OTNodeServletTest {
	private static final String HOST = "http://localhost/";
	private static final StructuredCodec<CommitId> REVISION_CODEC = REGISTRY.get(CommitId.class);
	private static final StructuredCodec<TestOp> DIFF_CODEC = TEST_OP_CODEC;
	private static final SimKey SIM_KEY = SimKey.generate();
	private static final StructuredCodec<FetchData<CommitId, TestOp>> FETCH_DATA_CODEC = FetchData.codec(REVISION_CODEC, DIFF_CODEC);
	private static final OTSystem<TestOp> OT_SYSTEM = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private OTRepositoryAdapter<TestOp> adapter;
	private OTRepositoryStub<CommitId, TestOp> repository;
	private AsyncServlet servlet;

	@Before
	public void setUp() {
		OTDriver driver = new OTDriver(null, SIM_KEY);
		adapter = new OTRepositoryAdapter<>(driver, new MyRepositoryId<>(null, null, DIFF_CODEC), emptySet());
		repository = OTRepositoryStub.create();
		repository.setCommitFactory(adapter);
		repository.setGraph(g -> {
			g.add(getCommitId(1), getCommitId(2), add(1));
			g.add(getCommitId(2), getCommitId(3), set(-12, 34));
			g.add(getCommitId(3), getCommitId(4), add(-12));
			g.add(getCommitId(4), getCommitId(5), set(4, 5));
		});
		OTNode<CommitId, TestOp, OTCommit<CommitId, TestOp>> node = OTNodeImpl.create(repository, OT_SYSTEM);
		servlet = OTNodeServlet.forGlobalNode(node, DIFF_CODEC, adapter);
	}

	@Test
	public void testCheckout() throws ParseException {
		HttpResponse response = await(servlet.serve(get(HOST + CHECKOUT)));
		ByteBuf body = await(response.getBody());
		String bodyString = body.asString(UTF_8);
		System.out.println(bodyString);

		FetchData<CommitId, TestOp> checkoutData = fromJson(FETCH_DATA_CODEC, bodyString);
		assertEquals(getCommitId(5), checkoutData.getCommitId());
		assertEquals(OT_SYSTEM.squash(asList(add(1), set(-12, 34), add(-12), set(4, 5))), checkoutData.getDiffs());
	}

	@Test
	public void testFetch() throws ParseException {
		String revisionFromJson = toJson(REGISTRY.get(CommitId.class), getCommitId(3));
		String urlEncoded = UrlBuilder.urlEncode(revisionFromJson);
		HttpResponse response = await(servlet.serve(get(HOST + FETCH + "?id=" + urlEncoded)));
		ByteBuf body = await(response.getBody());
		String bodyString = body.asString(UTF_8);
		System.out.println(bodyString);

		FetchData<CommitId, TestOp> fetchData = fromJson(FETCH_DATA_CODEC, bodyString);
		assertEquals(getCommitId(5), fetchData.getCommitId());
		assertEquals(OT_SYSTEM.squash(asList(add(-12), set(4, 5))), fetchData.getDiffs());
	}

	@Test
	public void testCreateCommit() throws ParseException {
		CommitId parent = getCommitId(5);
		List<TestOp> diffs = singletonList(add(100));

		FetchData<CommitId, TestOp> commit = new FetchData<>(parent, parent.getLevel(), diffs);
		HttpResponse response = await(servlet.serve(post(HOST + CREATE_COMMIT)
				.withBody(toJson(FETCH_DATA_CODEC, commit).getBytes(UTF_8))));
		ByteBuf body = await(response.getBody());
		byte[] bytes = body.asArray();
		OTCommit<CommitId, TestOp> otCommit = adapter.parseRawBytes(bytes);
		assertEquals(map(parent, diffs), otCommit.getParents());
		assertEquals(CommitId.of(parent.getLevel() + 1, sha256(bytes)), otCommit.getId());
	}

	@Test
	public void testPush() throws ParseException {
		CommitId parent = getCommitId(5);
		List<TestOp> diffs = singletonList(add(100));

		FetchData<CommitId, TestOp> commit = new FetchData<>(parent, parent.getLevel(), diffs);
		HttpResponse response = await(servlet.serve(post(HOST + CREATE_COMMIT)
				.withBody(toJson(FETCH_DATA_CODEC, commit).getBytes(UTF_8))));
		byte[] bytes = await(response.getBody()).asArray();
		response = await(servlet.serve(post(HOST + PUSH)
				.withBody(bytes)));

		ByteBuf body = await(response.getBody());
		String bodyString = body.asString(UTF_8);
		FetchData<CommitId, TestOp> fetchData = fromJson(FETCH_DATA_CODEC, bodyString);
		CommitId commitId = fetchData.getCommitId();
		assertEquals(adapter.parseRawBytes(bytes).getId(), commitId);

		Set<CommitId> heads = await(repository.getHeads());
		assertEquals(singleton(commitId), heads);

		OTCommit<CommitId, TestOp> headCommit = await(repository.loadCommit(commitId));
		assertEquals(map(parent, diffs), headCommit.getParents());
		assertEquals(parent.getLevel() + 1, headCommit.getLevel());
	}

}
