import dao.ArticleDao;
import dao.ot.ArticleDaoOT;
import dao.ot.ArticleOTState;
import dao.ot.operation.ArticleOperation;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.*;
import io.datakernel.util.ref.RefLong;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.SharedKeyStorage;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.ot.api.CommitId;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import io.global.ot.server.CommitStorage;
import io.global.ot.server.CommitStorageRocksDb;
import io.global.ot.server.GlobalOTNodeImpl;

import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static dao.ot.Utils.ARTICLE_OPERATION_CODEC;
import static dao.ot.Utils.createOTSystem;
import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.CollectionUtils.map;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

public final class OTModule extends AbstractModule {
	private static final String DEMO_NAME = "Blog";
	private static final RawServerId DEMO_RAW_SERVER_ID = new RawServerId(DEMO_NAME);
	private static final KeyPair DEMO_KEYS = PrivKey.of(BigInteger.ONE).computeKeys();
	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	private static final OTSystem<ArticleOperation> OT_SYSTEM = createOTSystem();
	private static final String OT_STORAGE = System.getProperty("java.io.tmpdir") + "/blog-ot-storage";

	@Provides
	ArticleDao articleDaoOT(OTStateManager<CommitId, ArticleOperation> stateManager, AsyncSupplier<Long> idGenerator) {
		return new ArticleDaoOT(stateManager, idGenerator);
	}

	@Provides
	AsyncSupplier<Long> idGeneratorStub(OTStateManager<CommitId, ArticleOperation> otStateManager) {
		RefLong idRef = new RefLong(-1);
		return () -> {
			if (idRef.value == -1) {
				Set<Long> ids = ((ArticleOTState) otStateManager.getState()).getArticles().keySet();
				idRef.value = ids.isEmpty() ? 0 : first(ids);
			}
			return Promise.of(++idRef.value);
		};
	}

	@Provides
	OTStateManager<CommitId, ArticleOperation> stateManager(Eventloop eventloop, OTRepository<CommitId, ArticleOperation> repository) {
		OTNodeImpl<CommitId, ArticleOperation, OTCommit<CommitId, ArticleOperation>> node = OTNodeImpl.create(repository, OT_SYSTEM);
		return OTStateManager.create(eventloop, OT_SYSTEM, node, new ArticleOTState());
	}

	@Provides
	OTRepository<CommitId, ArticleOperation> repository(GlobalOTNode globalOTNode) {
		OTDriver driver = new OTDriver(globalOTNode, DEMO_SIM_KEY);
		RepoID repoID = RepoID.of(DEMO_KEYS, DEMO_NAME);
		MyRepositoryId<ArticleOperation> myRepositoryId = new MyRepositoryId<>(repoID, DEMO_KEYS.getPrivKey(), ARTICLE_OPERATION_CODEC);
		return new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
	}

	@Provides
	CommitStorage commitStorage(Eventloop eventloop, Executor executor) {
		return CommitStorageRocksDb.create(executor, eventloop, OT_STORAGE);
	}

	@Provides
	GlobalOTNode globalOTNode(Eventloop eventloop, CommitStorage commitStorage, DiscoveryService discoveryService) {
		Function<RawServerId, GlobalOTNode> nodeFactory = $ -> {
			throw new AssertionError();
		};
		return GlobalOTNodeImpl.create(eventloop, DEMO_RAW_SERVER_ID, discoveryService, commitStorage, nodeFactory);
	}

	@Provides
	DiscoveryService discoveryService(Eventloop eventloop) {
		InMemoryAnnouncementStorage announcementStorage = new InMemoryAnnouncementStorage();
		SharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();

		AnnounceData announceData = new AnnounceData(System.currentTimeMillis(), singleton(DEMO_RAW_SERVER_ID));
		SignedData<AnnounceData> signedData = SignedData.sign(REGISTRY.get(AnnounceData.class), announceData, DEMO_KEYS.getPrivKey());
		announcementStorage.addAnnouncements(map(DEMO_KEYS.getPubKey(), signedData));

		return LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);
	}
}
