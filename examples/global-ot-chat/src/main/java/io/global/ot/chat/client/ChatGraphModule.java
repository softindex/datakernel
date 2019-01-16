package io.global.ot.chat.client;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncServlet;
import io.datakernel.ot.OTRepository;
import io.global.common.PrivKey;
import io.global.common.SimKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.chat.operations.ChatOperation;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.http.GlobalOTNodeHttpClient;

import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.datakernel.http.HttpUtils.urlDecode;
import static io.global.launchers.GlobalConfigConverters.ofPrivKey;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;
import static io.global.launchers.ot.GlobalOTConfigConverters.ofRepoID;
import static java.util.Collections.emptySet;

public class ChatGraphModule extends AbstractModule {
	private static final Function<CommitId, String> idToString = item -> DatatypeConverter.printHexBinary(item.toBytes()).toLowerCase().substring(0, 7);
	private static final Function<ChatOperation, String> diffToString = op -> (op.isTombstone() ? "-" : "+") + '[' + op.getContent() + ']';
	private static final OTGraphServlet<CommitId, ChatOperation> graphServlet = OTGraphServlet.create(idToString, diffToString);
	private static final Map<String, OTRepository<CommitId, ChatOperation>> repositoryCache = new HashMap<>();

	@Provides
	@Singleton
	@Named("Graph")
	AsyncServlet provideGraphServlet(Eventloop eventloop, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey");
		RepoID repoID = config.get(ofRepoID(), "credentials.repositoryId");
		PrivKey privKey = config.get(ofPrivKey(), "credentials.privateKey");
		MyRepositoryId<ChatOperation> myRepositoryId = new MyRepositoryId<>(repoID, privKey, ChatOperation.OPERATION_CODEC);

		return request -> {
			try {
				String nodeAddress = urlDecode(request.getQueryParameter("node"), "UTF-8");
				OTRepository<CommitId, ChatOperation> repository = getRepository(eventloop, nodeAddress, simKey, myRepositoryId);
				graphServlet.changeRepository(repository);
				return graphServlet.serve(request);
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};
	}

	private OTRepository<CommitId, ChatOperation> getRepository(Eventloop eventloop, String nodeAddress, SimKey simKey, MyRepositoryId<ChatOperation> myRepositoryId) {
		return repositoryCache.computeIfAbsent(nodeAddress, address -> {
			GlobalOTNodeHttpClient service = GlobalOTNodeHttpClient.create(AsyncHttpClient.create(eventloop), address);
			OTDriver driver = new OTDriver(service, simKey);
			return new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
		});
	}
}
