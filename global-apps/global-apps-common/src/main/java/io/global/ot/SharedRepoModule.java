package io.global.ot;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.ot.OTState;
import io.datakernel.ot.OTSystem;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.debug.ObjectDisplayRegistry;
import io.global.debug.ObjectDisplayRegistryUtils.*;
import io.global.kv.api.GlobalKvNode;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.RepoSynchronizer;
import io.global.ot.contactlist.ContactsOTState;
import io.global.ot.contactlist.ContactsOTSystem;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.service.messaging.MessagingService;
import io.global.ot.shared.SharedReposOTState;
import io.global.ot.shared.SharedReposOTSystem;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.Messenger;

import java.time.Duration;
import java.util.Random;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodecs.LONG_CODEC;
import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.global.debug.ObjectDisplayRegistry.merge;
import static io.global.debug.ObjectDisplayRegistryUtils.*;
import static io.global.ot.OTUtils.SHARED_REPO_MESSAGE_CODEC;

public abstract class SharedRepoModule<D> extends AbstractModule {
	@Override
	protected void configure() {
		bind(new Key<OTSystem<SharedReposOperation>>() {}).toInstance(SharedReposOTSystem.createOTSystem()).asEager();
		bind(new Key<OTSystem<ContactsOperation>>() {}).toInstance(ContactsOTSystem.createOTSystem()).asEager();
		bind(new Key<OTState<ContactsOperation>>() {}).to(ContactsOTState.class).in(ContainerScope.class);
	}

	@Provides
	@Eager
	@Named("poll interval")
	Duration pollInterval(Config config) {
		return config.get(ofDuration(), "message.poll.interval", MessagingService.DEFAULT_POLL_INTERVAL);
	}

	@Provides
	@ContainerScope
	Function<String, RepoSynchronizer<D>> repoSynchronizerFactory(Eventloop eventloop, OTDriver driver, KeyPair keys, StructuredCodec<D> diffCodec,
			OTSystem<D> system, Key<D> key, TypedRepoNames names) {
		return name -> RepoSynchronizer.create(eventloop, driver, system, MyRepositoryId.of(keys.getPrivKey(),
				names.getRepoPrefix(key) + name, diffCodec));
	}

	@Provides
	@ContainerScope
	OTState<SharedReposOperation> sharedReposOTState() {
		return new SharedReposOTState();
	}

	@Provides
	@ContainerScope
	ContactsOTState contactsOTState() {
		return new ContactsOTState();
	}

	@Provides
	Messenger<Long, CreateSharedRepo> messenger(GlobalKvNode node) {
		Random random = new Random();
		return Messenger.create(node, LONG_CODEC, SHARED_REPO_MESSAGE_CODEC, random::nextLong);
	}

	@Provides
	@ContainerScope
	ObjectDisplayRegistry objectDisplayRegistry() {
		return merge(forProfileOperation(), forContactsOperation());
	}

	@Provides
	@ContainerScope
	ObjectDisplayNameProvider objectDisplayNameProvider(KeyPair keys, ContactsOTState contactsState){
		return new ObjectDisplayNameProvider() {
			@Override
			public String getShortName(PubKey pubKey) {
				return getName(pubKey, pubKey.asString().substring(0, 7));
			}

			@Override
			public String getLongName(PubKey pubKey) {
				return getName(pubKey, pk(pubKey));
			}

			private String getName(PubKey pubKey, String fallbackName) {
				if (keys.getPubKey().equals(pubKey)) {
					return "'Myself'";
				}
				String name = contactsState.getContacts().get(pubKey);
				if (name != null) {
					return '\'' + name + '\'';
				}
				return fallbackName;
			}
		};
	}
}
