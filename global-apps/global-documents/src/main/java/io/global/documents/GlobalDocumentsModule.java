package io.global.documents;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.ot.OTSystem;
import io.global.debug.ObjectDisplayRegistry;
import io.global.debug.ObjectDisplayRegistryUtils.ObjectDisplayNameProvider;
import io.global.ot.DynamicOTUplinkServlet;
import io.global.ot.OTUtils;
import io.global.ot.TypedRepoNames;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.edit.EditOTSystem;
import io.global.ot.edit.EditOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.SharedUserContainer;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.shared.SharedReposOperation;

import java.time.Duration;

import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.global.Utils.cachedContent;
import static io.global.debug.ObjectDisplayRegistryUtils.forEditOperation;
import static io.global.debug.ObjectDisplayRegistryUtils.forSharedRepo;
import static io.global.ot.OTUtils.EDIT_OPERATION_CODEC;
import static io.global.ot.OTUtils.SHARED_REPO_MESSAGE_CODEC;
import static io.global.ot.client.RepoSynchronizer.DEFAULT_INITIAL_DELAY;

public final class GlobalDocumentsModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(new Key<SharedUserContainer<EditOperation>>() {}).in(ContainerScope.class);
		bind(Key.of(String.class).named("mail box")).toInstance("global-documents");
		bind(new Key<OTSystem<EditOperation>>() {}).toInstance(EditOTSystem.createOTSystem());
	}

	@Provides
	CodecFactory codecFactory() {
		return OTUtils.createOTRegistry()
				.with(EditOperation.class, EDIT_OPERATION_CODEC)
				.with(CreateSharedRepo.class, SHARED_REPO_MESSAGE_CODEC);
	}

	@Provides
	@Eager
	@Named("initial backoff")
	Duration initialBackOff(Config config) {
		return config.get(ofDuration(), "sync.initialBackoff", DEFAULT_INITIAL_DELAY);
	}

	@Provides
	AsyncServlet containerServlet(
			DynamicOTUplinkServlet<ContactsOperation> contactsServlet,
			DynamicOTUplinkServlet<SharedReposOperation> documentListServlet,
			DynamicOTUplinkServlet<EditOperation> documentServlet,
			DynamicOTUplinkServlet<MapOperation<String, String>> profileServlet,
			@Named("authorization") RoutingServlet authorizationServlet,
			@Named("session") AsyncServletDecorator sessionDecorator,
			StaticServlet staticServlet,
			@Optional @Named("debug") AsyncServlet debugServlet
	) {
		RoutingServlet routingServlet = RoutingServlet.create()
				.map("/ot/*", sessionDecorator.serve(RoutingServlet.create()
						.map("/contacts/*", contactsServlet)
						.map("/documents/*", documentListServlet)
						.map("/document/:suffix/*", documentServlet)
						.map("/profile/:pubKey/*", profileServlet)
						.map("/myProfile/*", profileServlet)))
				.map("/static/*", cachedContent().serve(staticServlet))
				.map("/*", staticServlet)
				.merge(authorizationServlet);
		if (debugServlet != null){
			routingServlet.map("/debug/*", debugServlet);
		}
		return routingServlet;
	}

	@Provides
	@Named("repo prefix")
	String repoPrefix(TypedRepoNames names) {
		return names.getRepoPrefix(Key.of(EditOperation.class));
	}

	@Provides
	@ContainerScope
	ObjectDisplayRegistry objectDisplayRegistry(ObjectDisplayNameProvider nameProvider) {
		return ObjectDisplayRegistry.merge(forEditOperation(), forSharedRepo("document", nameProvider));
	}
}
