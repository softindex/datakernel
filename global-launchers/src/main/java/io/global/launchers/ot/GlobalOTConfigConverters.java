package io.global.launchers.ot;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverter;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;

import static io.datakernel.config.ConfigConverters.ofString;
import static io.global.launchers.GlobalConfigConverters.ofPrivKey;

public class GlobalOTConfigConverters {
	public static ConfigConverter<RepoID> ofRepoID() {
		return ofString().transform(RepoID::fromString, RepoID::asString);
	}

	public static <D> ConfigConverter<MyRepositoryId<D>> ofMyRepositoryId(StructuredCodec<D> diffCodec) {
		return new ConfigConverter<MyRepositoryId<D>>() {
			@Override
			public MyRepositoryId<D> get(Config config, @Nullable MyRepositoryId<D> defaultValue) {
				try {
					return get(config);
				} catch (NoSuchElementException | IllegalArgumentException ignored) {
					return defaultValue;
				}
			}

			@NotNull
			@Override
			public MyRepositoryId<D> get(Config config) {
				return new MyRepositoryId<>(
						config.get(ofRepoID(), "repoId"),
						config.get(ofPrivKey(), "privateKey"),
						diffCodec
				);
			}
		};
	}
}
