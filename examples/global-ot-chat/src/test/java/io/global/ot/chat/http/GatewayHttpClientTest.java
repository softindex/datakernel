package io.global.ot.chat.http;

import io.datakernel.async.Promise;
import io.datakernel.http.AsyncServlet;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.util.Tuple2;
import io.global.ot.api.CommitId;
import io.global.ot.chat.common.Gateway;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(DatakernelRunner.DatakernelRunnerFactory.class)
public class GatewayHttpClientTest {
	private static final Random RANDOM = new Random();
	private static final LinkedList<Object> params = new LinkedList<>();
	private final AsyncServlet servlet = getServlet();
	private final GatewayHttpClient<String> client = GatewayHttpClient.create(servlet::serve, "http://localhost/", STRING_CODEC);

	@Parameterized.Parameter()
	public static List<String> strings;

	@Parameters
	public static Collection<Object[]> getparameters() {
		return asList(
				new Object[]{emptyList()},
				new Object[]{asList("Hello", "World", "", "    ", "!")}
		);
	}

	@Test
	public void checkout() {
		doTest(client.checkout());
	}

	@Test
	public void pull() {
		CommitId commitId = randomCommitId();
		doTest(client.pull(commitId), commitId);
	}

	@Test
	public void push() {
		CommitId commitId = randomCommitId();
		doTest(client.push(commitId, strings), commitId, strings);
	}

	private static <T> void doTest(Promise<T> promise, Object... parameters) {
		T result = await(promise);
		assertEquals(params.remove(), result);
		for (Object param : parameters) {
			assertEquals(params.remove(), param);
		}
		assertTrue(params.isEmpty());
	}

	// region helpers
	private static AsyncServlet getServlet() {
		return GatewayServlet.create(new Gateway<String>() {
			<T> Promise<T> resultOf(@Nullable T result, Object... args) {
				params.add(result);
				params.addAll(asList(args));
				return Promise.of(result);
			}

			@Override
			public Promise<Tuple2<CommitId, List<String>>> checkout() {
				return resultOf(new Tuple2<>(randomCommitId(), strings));
			}

			@Override
			public Promise<Tuple2<CommitId, List<String>>> pull(CommitId oldId) {
				return resultOf(new Tuple2<>(randomCommitId(), strings), oldId);
			}

			@Override
			public Promise<CommitId> push(CommitId currentId, List<String> clientDiffs) {
				return resultOf(randomCommitId(), currentId, clientDiffs);
			}
		}, STRING_CODEC);
	}

	private static CommitId randomCommitId() {
		byte[] bytes = new byte[32];
		RANDOM.nextBytes(bytes);
		return CommitId.ofBytes(bytes);
	}
	// endregion
}
