import io.datakernel.async.Promise;
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
import io.datakernel.memcache.protocol.MemcacheRpcMessage.Slice;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.CompletionStage;

import static io.datakernel.di.module.Modules.combine;
import static java.nio.charset.StandardCharsets.UTF_8;

//[START REGION_1]
public class MemcacheLikeClient extends Launcher {
	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	Config config() {
		return Config.create()
				.with("protocol.compression", "false")
				.with("client.addresses", "localhost:9010, localhost:9020, localhost:9030")
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}

	@Inject
	RawMemcacheClient client;

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
	protected void run() {
		eventloop.submit(() -> {
			byte[][] bytes = new byte[25][1];
			for (int i = 0; i < 25; ++i) {
				bytes[i] = new byte[]{(byte) i};
			}
			for (int i = 0; i < 25; ++i) {
				final int idx = i;
				byte[] message = "Strong and smart people always be successful".getBytes();
				Promise<Void> put = client.put(bytes[i], new Slice(message));
				put.whenComplete((res, e) -> {
					if (e != null) {
						e.printStackTrace();
						return;
					}
					System.out.println("Request sent : " + idx);
				});
			}
			for (int i = 0; i < 25; ++i) {
				final int idx = i;
				Promise<Slice> byteBufPromise = client.get(bytes[idx]);
				byteBufPromise.whenComplete((resp, e1) -> {
					if (e1 != null) {
						e1.printStackTrace();
						return;
					}
					System.out.println("Got back from [" + idx + "] : " +
							new String(resp.array(), resp.offset(), resp.length(), UTF_8));
				});
			}
		});
	}

	public static void main(String[] args) throws Exception {
		Launcher client = new MemcacheLikeClient();
		client.launch(args);
	}
}
//[END REGION_1]
