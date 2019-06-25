/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.config;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Multibinder;
import io.datakernel.launcher.OnStart;
import io.datakernel.util.Initializable;
import io.datakernel.util.Initializer;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;

/**
 * Supplies config to your application, looks after usage of config, prevents usage of config in any part of lifecycle except for startup.
 */
@SuppressWarnings("unused")
public final class ConfigModule extends AbstractModule implements Initializable<ConfigModule> {
	private static final Logger logger = LoggerFactory.getLogger(ConfigModule.class);
	public static final Key<Config> KEY_OF_CONFIG = Key.of(Config.class);

	@Nullable
	private Supplier<Config> configSupplier;
	private Path effectiveConfigPath;
	private Consumer<String> effectiveConfigConsumer;

	static final class ProtectedConfig implements Config {
		private final Config config;
		private final Map<String, Config> children;
		private final AtomicBoolean started;

		ProtectedConfig(Config config, AtomicBoolean started) {
			this.config = config;
			this.started = started;
			this.children = new LinkedHashMap<>();
			config.getChildren().forEach((key, value) ->
					this.children.put(key, new ProtectedConfig(value, started)));
		}

		@Override
		public String getValue(String defaultValue) {
			checkState(!started.get(), "Config must be used during application start-up time only");
			return config.getValue(defaultValue);
		}

		@Override
		public String getValue() throws NoSuchElementException {
			checkState(!started.get(), "Config must be used during application start-up time only");
			return config.getValue();
		}

		@Override
		public Map<String, Config> getChildren() {
			return children;
		}

		@Override
		public Config provideNoKeyChild(String key) {
			checkArgument(!children.keySet().contains(key), "Children already contain key '%s'", key);
			return new ProtectedConfig(config.provideNoKeyChild(key), started);
		}
	}

	private ConfigModule(@Nullable Supplier<Config> configSupplier) {
		this.configSupplier = configSupplier;
	}

	public static ConfigModule create() {
		return new ConfigModule(null);
	}

	public static ConfigModule create(Supplier<Config> configSupplier) {
		return new ConfigModule(configSupplier);
	}

	public static ConfigModule create(Config config) {
		return new ConfigModule(() -> config);
	}

	public ConfigModule saveEffectiveConfigTo(String file) {
		return saveEffectiveConfigTo(Paths.get(file));
	}

	public ConfigModule saveEffectiveConfigTo(Path file) {
		this.effectiveConfigPath = file;
		return this;
	}

	public ConfigModule withEffectiveConfigConsumer(Consumer<String> consumer) {
		this.effectiveConfigConsumer = consumer;
		return this;
	}

	public ConfigModule writeEffectiveConfigTo(Writer writer) {
		return withEffectiveConfigConsumer(effectiveConfig -> {
			try {
				writer.write(effectiveConfig);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public ConfigModule writeEffectiveConfigTo(PrintStream writer) {
		return withEffectiveConfigConsumer(writer::print);
	}

	public ConfigModule printEffectiveConfig() {
		return withEffectiveConfigConsumer(effectiveConfig ->
				logger.info("Effective Config:\n\n" + effectiveConfig));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void configure() {
		multibind(new Key<Set<Initializer<ConfigModule>>>() {}, Multibinder.toSet());

		if (configSupplier != null) {
			bind(Config.class).toInstance(configSupplier.get());
		}

		transform(0, (provider, scope, key, binding) -> {
			if (!key.equals(KEY_OF_CONFIG)) return binding;
			return ((Binding<Config>) (Binding) binding)
					.addDependencies(new Key<CompletionStage<Void>>(OnStart.class) {})
					.mapInstance((args, config) -> {
						CompletionStage<Void> onStart = (CompletionStage<Void>) args[args.length - 1];
						AtomicBoolean started = new AtomicBoolean();
						ProtectedConfig protectedConfig = new ProtectedConfig(ConfigWithFullPath.wrap(config), started);
						EffectiveConfig effectiveConfig = EffectiveConfig.wrap(protectedConfig);
						onStart.thenRun(() -> save(effectiveConfig, started));
						return effectiveConfig;
					});
		});
	}

	private void save(EffectiveConfig effectiveConfig, AtomicBoolean started) {
		started.set(true);
		if (effectiveConfigPath != null) {
			logger.info("Saving effective config to {}", effectiveConfigPath);
			effectiveConfig.saveEffectiveConfigTo(effectiveConfigPath);
		}
		if (effectiveConfigConsumer != null) {
			effectiveConfigConsumer.accept(effectiveConfig.renderEffectiveConfig());
		}
	}

}
