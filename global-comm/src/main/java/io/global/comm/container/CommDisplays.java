package io.global.comm.container;

import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.global.comm.ot.post.operation.*;
import io.global.comm.pojo.*;
import io.global.comm.util.PagedAsyncMap;
import io.global.debug.ObjectDisplayRegistry;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.service.ContainerScope;
import io.global.ot.session.UserId;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public final class CommDisplays extends AbstractModule {

	@Provides
	@ContainerScope
	ObjectDisplayRegistry diffDisplay(PagedAsyncMap<UserId, UserData> users) {
		return ObjectDisplayRegistry.create()
				.withDisplay(AddPost.class,
						($, p) -> (p.isRemove() ? "remove" : "add") + (p.getParentId() == null ? " root" : "") + " post",
						(d, p) -> "add " + (post(p.getPostId()) + (p.getParentId() == null ? "" : "(child of " + post(p.getParentId()) + ")")) + " by " + id(p.getAuthor(), users) + " " + ts(p.getInitialTimestamp()))
				.withDisplay(DeletePost.class,
						($, p) -> (p.isDelete() ? "" : "un") + "mark post as deleted",
						(d, p) -> (p.isDelete() ? "" : "un") + "mark " + post(p.getPostId()) + " as deleted by " + id(p.getDeletedBy(), users) + " " + ts(p.getTimestamp()))
				.withDisplay(ChangeContent.class,
						($, p) -> (nullOrEmpty(p.getPrev()) ? "add" : "edit") + ("root".equals(p.getPostId()) ? " root" : "") + " post content",
						(d, p) -> (nullOrEmpty(p.getPrev()) ? "set content of " + post(p.getPostId()) : "change content of " + post(p.getPostId()) + " from " + text(p.getPrev())) + " to " + text(p.getNext()) + " " + ts(p.getTimestamp()))

				.withDisplay(ChangeLastEditTimestamp.class,
						($, p) -> "update last edit time",
						(d, p) -> "set last edit time of " + post(p.getPostId()) + (p.getPrevTimestamp() == -1 ? "" : " from " + ts(p.getPrevTimestamp())) + " to " + ts(p.getNextTimestamp()))
				.withDisplay(ChangeAttachments.class,
						($, p) -> (p.isRemove() ? "remove " : "add ") + p.getAttachmentType().name().toLowerCase() + " attachment",
						(d, p) -> d.getShortDisplay(p) + " " + file(p.getPostId(), p.getFilename()) + " to " + post(p.getPostId()) + " " + ts(p.getTimestamp()))
				.withDisplay(ChangeRating.class,
						($, p) -> (p.getSetRating().getNext() == null ? "remove rating" : p.getSetRating().getNext().name().toLowerCase()),
						(d, p) -> "change rating of " + post(p.getPostId()) + " from " + rating(p.getSetRating().getPrev()) +
								" to " + rating(p.getSetRating().getNext()) + " by " + id(p.getUserId(), users))
				.withDisplay(PostChangesOperation.class,
						(d, p) -> Stream.of(p.getDeletePostOps(), p.getChangeContentOps(), p.getChangeLastEditTimestamps(), p.getChangeAttachmentsOps(), p.getChangeRatingOps())
								.flatMap(ops -> ops.stream().map(d::getShortDisplay))
								.collect(joining("\n")),
						(d, p) -> Stream.of(p.getDeletePostOps(), p.getChangeContentOps(), p.getChangeLastEditTimestamps(), p.getChangeAttachmentsOps(), p.getChangeRatingOps())
								.flatMap(ops -> ops.stream().map(d::getLongDisplay))
								.collect(joining("\n")))

				.withDisplay(new Key<MapOperation<String, ThreadMetadata>>() {},
						(d, p) -> p.getOperations().values().stream()
								.map(threadMeta -> setOperation(threadMeta, v -> "thread " + v.getTitle(), ($, $2) -> ""))
								.collect(joining("\n")),
						(d, p) -> p.getOperations().entrySet().stream()
								.map(e -> setOperation(e.getValue(),
										v -> thread(e.getKey()),
										v -> thread(e.getKey()) + ", title: " + text(v.getTitle()),
										(prev, next) -> {
											String s = "";
											if (!prev.getTitle().equals(next.getTitle())) {
												s += nullOrEmpty(prev.getTitle()) ?
														", set title to " + text(next.getTitle()) :
														", title from " + text(prev.getTitle()) + " to " + text(next.getTitle());
											}
											if (prev.getLastUpdate() != next.getLastUpdate()) {
												s += prev.getLastUpdate() == 0 ?
														", set last update timestamp to " + ts(next.getLastUpdate()) :
														", last update timestamp from " + ts(prev.getLastUpdate()) + " to " + ts(next.getLastUpdate());
											}
											return s;
										}))
								.collect(joining("\n")))
				.withDisplay(new Key<MapOperation<UserId, UserData>>() {},
						(d, p) -> p.getOperations().values().stream()
								.map(userData -> setOperation(userData, v -> v.getUsername() != null ? "user " + v.getUsername() : "some user", $ -> "", ($, $2) -> ""))
								.collect(joining("\n")),
						(d, p) -> p.getOperations().entrySet().stream()
								.map(e -> setOperation(e.getValue(),
										v -> "user " + id(e.getKey(), v),
										v -> {
											List<String> details = new ArrayList<>();
											if (nullOrEmpty(v.getEmail())) {
												details.add("email: " + special(v.getEmail()));
											}
											if (nullOrEmpty(v.getFirstName())) {
												details.add("first name: " + special(v.getFirstName()));
											}
											if (nullOrEmpty(v.getLastName())) {
												details.add("last name: " + special(v.getLastName()));
											}
											BanState banState = v.getBanState();
											if (banState != null) {
												details.add("ban state: " + banState(banState, users));
											}
											return " " + (details.isEmpty() ? "" : "(" + String.join(", ", details) + ")");
										},
										(prev, next) -> {
											List<String> changes = new ArrayList<>();
											if (!Objects.equals(prev.getEmail(), next.getEmail())) {
												changes.add("email: " + special(prev.getEmail()) + " -> " + special(next.getEmail()));
											}
											if (!Objects.equals(prev.getFirstName(), next.getFirstName())) {
												changes.add("first name: " + special(prev.getFirstName()) + " -> " + special(next.getFirstName()));
											}
											if (!Objects.equals(prev.getLastName(), next.getLastName())) {
												changes.add("last name: " + special(prev.getLastName()) + " -> " + special(next.getLastName()));
											}
											if (!Objects.equals(prev.getBanState(), next.getBanState())) {
												changes.add("ban state: set to " + (next.getBanState() == null ? special(null) : banState(next.getBanState(), users)));
											}
											return (" " + (Objects.equals(prev.getUsername(), next.getUsername()) ? "" : "(was " + id(e.getKey(), prev) + ")")) +
													(changes.isEmpty() ? "" : "(" + String.join(", ", changes) + ")");
										}))
								.collect(joining("\n")))
				.withDisplay(new Key<MapOperation<UserId, InetAddress>>() {},
						(d, p) -> p.getOperations().entrySet().stream()
								.map(e -> setOperation(e.getValue(), v -> {
									UserData userData = users.get(e.getKey()).getResult();
									return "last IP of " + (nullOrEmpty(userData.getUsername()) ? "user " + userData.getUsername() : "some user");
								}, ($, $2) -> ""))
								.collect(joining("\n")),
						(d, p) -> p.getOperations().entrySet().stream()
								.map(e -> setOperation(e.getValue(),
										v -> "last IP of user " + id(e.getKey(), users),
										v -> "last IP of user " + id(e.getKey(), users) + " - " + special(v.toString()),
										(prev, next) -> " from " + special(prev.toString()) + " to " + special(next.toString())))
								.collect(joining("\n")))
				.withDisplay(new Key<MapOperation<String, IpBanState>>() {},
						(d, p) -> p.getOperations().values().stream()
								.map(ipBan -> setOperation(ipBan, v -> "IP ban for " + ipRange(v.getIpRange()), $ -> "", ($, $2) -> ""))
								.collect(joining("\n")),
						(d, p) -> p.getOperations().entrySet().stream()
								.map(e -> setOperation(e.getValue(),
										v -> "IP ban for " + ipRange(v.getIpRange()) + ", ban id " + special(e.getKey()),
										v -> "IP ban for " + ipRange(v.getIpRange()) + " " + banState(v.getBanState(), users) + ", ban id " + special(e.getKey()),
										(prev, next) -> {
											String s = "change IP ban " + ipRange(next.getIpRange());
											if (!prev.getBanState().getUntil().equals(next.getBanState().getUntil())) {
												s += " from " + ts(prev.getBanState().getUntil().toEpochMilli()) + " to " + ts(next.getBanState().getUntil().toEpochMilli());
											}
											if (!prev.getBanState().getReason().equals(next.getBanState().getReason())) {
												s += " reason from " + text(prev.getBanState().getReason()) + " to " + text(next.getBanState().getReason());
											}
											return s + ", ban id " + special(e.getKey());
										}))
								.collect(joining("\n")));
	}

	private static boolean nullOrEmpty(@Nullable String s) {
		return s == null || s.isEmpty();
	}

	private static String post(String postId) {
		return "root".equals(postId) ? "<span class=\"special\">root</span> post" : "post <span class=\"special\">" + postId + "</span>";
	}

	private static String thread(String threadId) {
		return "root".equals(threadId) ? "<span class=\"special\">root</span> thread" : "thread <span class=\"special\">" + threadId + "</span>";
	}

	public static String ts(long ts) {
		return "<span class=\"timestamp\">" + ts + "</span>";
	}

	private static String id(UserId id, @Nullable UserData userData) {
		String username = "<i>&lt;unknown&gt;</i>";
		if (userData != null) {
			if (nullOrEmpty(userData.getUsername())) {
				username = userData.getUsername();
			}
		}
		return "<span class=\"special\" title=\"" + id.getAuthId() + "\">" + username + "</span>";
	}

	private static String id(UserId id, PagedAsyncMap<UserId, UserData> users) {
		return id(id, users.get(id).getResult());
	}

	private static String rating(Rating rating) {
		return "<span class=\"special\">" + (rating == null ? "not rated" : rating.name().toLowerCase()) + "</span>";
	}

	private static String file(String postId, String filename) {
		return "<a class=\"special\" href=\"javascript:void(0)\" onclick=\"window.open(location.pathname.replace('ot/thread', 'fs')+'/" + postId + "/" + filename + "', '_blank')\">" + filename + "</a>";
	}

	public static String shortText(String text) {
		return (text.length() <= 20 ? text : text.substring(0, 17) + "...").replaceAll("\n", "\\n");
	}

	public static String text(String text) {
		if (text == null) {
			return special(null);
		}
		return "<span class=\"special\" title=\"" + text.replaceAll("\n", "\\\\\\\n") + "\">" +
				shortText(text).replaceAll("\\\\n", "<span class=\"newline\">\\\\\\\\n</span>") + "</span>";
	}

	public static String special(String text) {
		return text == null ? "<span class=\"null\">null</span>" : "<span class=\"special\">" + text + "</span>";
	}

	private static String banState(BanState banState, PagedAsyncMap<UserId, UserData> users) {
		return "banned by " + id(banState.getBanner(), users) + " until " + ts(banState.getUntil().toEpochMilli()) + "for " + text(banState.getReason());
	}

	private static String ipConcat(byte[] bs) {
		StringBuilder sb = new StringBuilder();
		for (byte b : bs) {
			sb.append(b).append('.');
		}
		sb.setLength(sb.length() - 2);
		return sb.toString();
	}

	private static String ipRange(IpRange ipRange) {
		byte[] mask = ipRange.getMask();
		for (byte b : mask) {
			if (b != -1) {
				return ipConcat(ipRange.getIp()) + "/" + ipConcat(ipRange.getMask());
			}
		}
		return ipConcat(ipRange.getIp());
	}

	private static <E> String setOperation(SetValue<E> p, Function<E, String> qualifier, BiFunction<E, E, String> change) {
		return setOperation(p, qualifier, qualifier, change);
	}

	private static <E> String setOperation(SetValue<E> p, Function<E, String> qualifier, Function<E, String> add, BiFunction<E, E, String> change) {
		return p.getNext() == null ?
				p.getPrev() == null ?
						"<empty set operation>" :
						"remove " + qualifier.apply(p.getPrev()) :
				p.getPrev() == null ?
						"add " + add.apply(p.getNext()) :
						"change " + qualifier.apply(p.getNext()) + change.apply(p.getPrev(), p.getNext());
	}
}
