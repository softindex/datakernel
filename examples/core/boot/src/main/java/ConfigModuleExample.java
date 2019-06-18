import io.datakernel.config.Config;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;

import java.net.InetAddress;

import static io.datakernel.config.ConfigConverters.ofInetAddress;
import static io.datakernel.config.ConfigConverters.ofInteger;
import static java.lang.Thread.currentThread;

public final class ConfigModuleExample extends AbstractModule {
	private static final String PROPERTIES_FILE = "example.properties";

	@Provides
	Config config() {
		return Config.ofClassPathProperties(currentThread().getContextClassLoader(), PROPERTIES_FILE);
	}

	@Provides
	String phrase(Config config) {
		return config.get("phrase");
	}

	@Provides
	Integer number(Config config) {
		return config.get(ofInteger(), "number");
	}

	@Provides
	InetAddress address(Config config) {
		return config.get(ofInetAddress(), "address");
	}

	public static void main(String[] args) {
		Injector injector = Injector.of(new ConfigModuleExample());

		String phrase = injector.getInstance(String.class);
		Integer number = injector.getInstance(Integer.class);
		InetAddress address = injector.getInstance(InetAddress.class);

		System.out.println(phrase);
		System.out.println(number);
		System.out.println(address);
	}
}
