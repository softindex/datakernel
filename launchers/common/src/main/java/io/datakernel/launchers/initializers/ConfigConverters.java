package io.datakernel.launchers.initializers;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverter;
import io.datakernel.dns.DnsCache;
import io.datakernel.eventloop.Eventloop;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.datakernel.dns.DnsCache.*;

public final class ConfigConverters {
	private ConfigConverters() {
		throw new AssertionError();
	}

	public static ConfigConverter<DnsCache> ofDnsCache(Eventloop eventloop) {
		return new ConfigConverter<DnsCache>() {
			@NotNull
			@Override
			public DnsCache get(Config config) {
				Duration errorCacheExpiration = config.get(ofDuration(), "errorCacheExpiration", DEFAULT_ERROR_CACHE_EXPIRATION);
				Duration hardExpirationDelta = config.get(ofDuration(), "hardExpirationDelta", DEFAULT_HARD_EXPIRATION_DELTA);
				Duration timedOutExceptionTtl = config.get(ofDuration(), "timedOutExceptionTtl", DEFAULT_TIMED_OUT_EXCEPTION_TTL);
				return DnsCache.create(eventloop)
						.withErrorCacheExpirationSeconds(errorCacheExpiration)
						.withHardExpirationDelta(hardExpirationDelta)
						.withTimedOutExceptionTtl(timedOutExceptionTtl);
			}

			@Nullable
			@Override
			@Contract("_, !null -> !null")
			public DnsCache get(Config config, @Nullable DnsCache defaultValue) {
				if (config.isEmpty()) {
					return defaultValue;
				} else {
					return get(config);
				}
			}
		};
	}
}
