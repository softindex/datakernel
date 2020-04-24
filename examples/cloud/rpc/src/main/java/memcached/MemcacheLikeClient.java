package memcached;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.ModuleBuilder;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.memcache.client.MemcacheClientModule;
import io.datakernel.memcache.client.RawMemcacheClient;
import io.datakernel.promise.Promises;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
				.with("client.addresses", "localhost:9000, localhost:9001, localhost:9002");
	}

	@Inject
	RawMemcacheClientAdapter client;

	@Inject
	Eventloop eventloop;

	@Override
	protected Module getModule() {
		return ModuleBuilder.create()
				.install(ServiceGraphModule.create())
				.install(MemcacheClientModule.create())
				.install(ConfigModule.create()
						.withEffectiveConfigLogger())
				.build();
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
