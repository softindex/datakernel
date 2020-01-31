package io.global.chat;

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
import io.global.chat.chatroom.CallInfo;
import io.global.chat.chatroom.operation.*;
import io.global.debug.ObjectDisplayRegistry;
import io.global.debug.ObjectDisplayRegistryUtils;
import io.global.kv.api.GlobalKvNode;
import io.global.ot.DynamicOTUplinkServlet;
import io.global.ot.OTUtils;
import io.global.ot.TypedRepoNames;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.SharedUserContainer;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.Messenger;
import io.global.pm.MessengerServlet;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.global.Utils.cachedContent;
import static io.global.chat.Utils.CHAT_ROOM_OPERATION_CODEC;
import static io.global.chat.Utils.CHAT_ROOM_OT_SYSTEM;
import static io.global.common.CryptoUtils.randomBytes;
import static io.global.common.CryptoUtils.toHexString;
import static io.global.debug.ObjectDisplayRegistryUtils.*;
import static io.global.ot.client.RepoSynchronizer.DEFAULT_INITIAL_DELAY;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

public final class GlobalChatModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(new Key<SharedUserContainer<ChatRoomOperation>>() {}).in(ContainerScope.class);
		bind(Key.of(String.class).named("mail box")).toInstance("global-chat");
		bind(new Key<OTSystem<ChatRoomOperation>>() {}).toInstance(CHAT_ROOM_OT_SYSTEM);
	}

	@Provides
	CodecFactory codecFactory() {
		return OTUtils.createOTRegistry()
				.with(ChatRoomOperation.class, CHAT_ROOM_OPERATION_CODEC);
	}

	@Provides
	AsyncServlet containerServlet(
			DynamicOTUplinkServlet<ContactsOperation> contactsServlet,
			DynamicOTUplinkServlet<SharedReposOperation> roomListServlet,
			DynamicOTUplinkServlet<ChatRoomOperation> roomServlet,
			DynamicOTUplinkServlet<MapOperation<String, String>> profileServlet,
			@Named("authorization") RoutingServlet authorizationServlet,
			@Named("session") AsyncServletDecorator sessionDecorator,
			Messenger<String, String> notificationsMessenger,
			StaticServlet staticServlet,
			@Optional @Named("debug") AsyncServlet debugServlet
	) {
		RoutingServlet routingServlet = RoutingServlet.create()
				.map("/ot/*", sessionDecorator.serve(RoutingServlet.create()
						.map("/contacts/*", contactsServlet)
						.map("/rooms/*", roomListServlet)
						.map("/room/:suffix/*", roomServlet)
						.map("/profile/:key/*", profileServlet)
						.map("/myProfile/*", profileServlet)))
				.map("/notifications/*", sessionDecorator.serve(MessengerServlet.create(notificationsMessenger)))
				.map("/static/*", cachedContent().serve(staticServlet))
				.map("/*", staticServlet)
				.merge(authorizationServlet);
		if (debugServlet != null) {
			routingServlet.map("/debug/*", debugServlet);
		}
		return routingServlet;
	}

	@Provides
	Messenger<String, String> notificationsMessenger(GlobalKvNode node) {
		return Messenger.create(node, STRING_CODEC, STRING_CODEC, () -> toHexString(randomBytes(8)));
	}

	@Provides
	@Named("repo prefix")
	String repoPrefix(TypedRepoNames names) {
		return names.getRepoPrefix(Key.of(ChatRoomOperation.class));
	}

	@Provides
	@Eager
	@Named("initial backoff")
	Duration initialBackOff(Config config) {
		return config.get(ofDuration(), "sync.initialBackoff", DEFAULT_INITIAL_DELAY);
	}

	@Provides
	@ContainerScope
	ObjectDisplayRegistry objectDisplayRegistry(ObjectDisplayNameProvider nameProvider) {
		return ObjectDisplayRegistryUtils.forSharedRepo("chat room", nameProvider)
				.withDisplay(MessageOperation.class,
						($, p) -> (p.isInvert() ?
								"Remove message by " :
								"Add new message from ")
								+ nameProvider.getShortName(p.getAuthor()),
						($, p) -> (p.isInvert() ?
								"Remove message by " :
								"Add new message from ")
								+ nameProvider.getLongName(p.getAuthor()) + " " + ts(p.getTimestamp()) + " with text '" +
								text(p.getContent()) + '\'')
				.withDisplay(CallOperation.class,
						($, p) -> p.getNext() == null ?
								"Invert a call operation" :
								("New call from " + nameProvider.getShortName(p.getNext().getPubKey())),
						($, p) -> {
							CallInfo prev = p.getPrev();
							CallInfo next = p.getNext();
							assert prev != null || next != null;
							if (next == null) {
								return "A call by " + nameProvider.getLongName(prev.getPubKey()) +
										" started " + ts(prev.getCallStarted()) +
										" was reverted";
							}
							if (prev == null) {
								return "A new call by " + nameProvider.getLongName(next.getPubKey()) +
										" started " + ts(next.getCallStarted());
							}

							return "A new call by " + nameProvider.getLongName(next.getPubKey()) +
									" started " + ts(next.getCallStarted()) +
									" has overridden a call by " + nameProvider.getLongName(prev.getPubKey()) +
									" made " + ts(prev.getCallStarted());
						})
				.withDisplay(DropCallOperation.class,
						($, p) -> !p.isInvert() ?
								("Drop a call by " + nameProvider.getShortName(p.getCallInfo().getPubKey())) :
								("Dropping a call by " + nameProvider.getShortName(p.getCallInfo().getPubKey()) + " was reverted"),
						($, p) -> {
							if (p.isInvert()) {
								return ("Drop a call by " + nameProvider.getLongName(p.getCallInfo().getPubKey()));
							} else {
								return "A call by " + nameProvider.getLongName(p.getCallInfo().getPubKey()) +
										" started " + ts(p.getCallInfo().getCallStarted()) +
										" was dropped " + ts(p.getDropTimestamp());
							}
						})
				.withDisplay(HandleCallOperation.class,
						($, p) -> {
							Map<Boolean, Long> processed = p.getMapOperation()
									.getOperations()
									.values()
									.stream()
									.map(SetValue::getNext)
									.filter(Objects::nonNull)
									.collect(groupingBy(identity(), counting()));
							Long accepted = processed.get(TRUE);
							Long rejected = processed.get(FALSE);
							if (accepted == null && rejected == null) {
								return "A call was not handled by anyone";
							}
							if (accepted == null) {
								return "A call was rejected by " + rejected + (rejected == 1 ? " person" : " people");
							}
							if (rejected == null) {
								return "A call was accepted by " + accepted + (accepted == 1 ? " person" : " people");
							}
							return "A call was accepted by " + accepted + (accepted == 1 ? " person" : " people") +
									", and rejected by " + rejected + (rejected == 1 ? " person" : " people");
						},
						($, p) -> p.getMapOperation().getOperations()
								.entrySet()
								.stream()
								.filter(e -> !e.getValue().isEmpty())
								.map(e -> {
									StringBuilder sb = new StringBuilder(nameProvider.getLongName(e.getKey()) + ", who has previously ");
									SetValue<Boolean> setValue = e.getValue();
									Boolean prev = setValue.getPrev();
									Boolean next = setValue.getNext();
									if (prev == null) {
										sb.insert(sb.indexOf("prev"), "not ");
										sb.append("handled ");
									}
									if (prev == FALSE) sb.append("rejected ");
									if (prev == TRUE) sb.append("accepted ");
									sb.append("the call, now ");

									if (next == null) sb.append("unhandled ");
									if (next == FALSE) sb.append("rejected ");
									if (next == TRUE) sb.append("accepted ");
									sb.append("it");
									return sb.toString();
								})
								.collect(joining("\n"))
				);
	}
}
