import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.memcache.client.MemcacheClientModule;
import io.datakernel.memcache.client.RawMemcacheClient;
import io.datakernel.promise.Promises;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.promise.Promises.sequence;
import static java.util.stream.IntStream.range;

//[START REGION_1]
public class MemcacheLikeClient extends Launcher {
	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RawMemcacheClientAdapter rawMemcacheClientAdapter(RawMemcacheClient client) {
		return new RawMemcacheClientAdapter(client);
	}

	@Provides
	Config config() {
		return Config.create()
				.with("protocol.compression", "false")
				.with("client.addresses", "localhost:9010, localhost:9020, localhost:9030")
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}

	@Inject
	RawMemcacheClientAdapter client;

	@Inject
	Eventloop eventloop;

	@Override
	protected Module getModule() {
		return combine(ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				MemcacheClientModule.create());
	}

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		String message = "Hello, Memcached Server";

		CompletableFuture<Void> future = eventloop.submit(() ->
				sequence(
						() -> Promises.all(range(0, 25).mapToObj(i ->
								client.put(i, message))),
						() -> Promises.all(range(0, 25).mapToObj(i ->
								client.get(i).whenResult(res -> System.out.println(i + " : " + res))))));
		future.get();
	}

	public static void main(String[] args) throws Exception {
		Launcher client = new MemcacheLikeClient();
		client.launch(args);
	}
}
//[END REGION_1]
