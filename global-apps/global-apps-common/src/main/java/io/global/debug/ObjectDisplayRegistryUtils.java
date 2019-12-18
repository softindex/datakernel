package io.global.debug;

import io.datakernel.di.core.Key;
import io.global.common.PubKey;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.edit.DeleteOperation;
import io.global.ot.edit.InsertOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.shared.CreateOrDropRepo;
import io.global.ot.shared.RenameRepo;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.joining;

public final class ObjectDisplayRegistryUtils {
	private ObjectDisplayRegistryUtils() {
		throw new AssertionError("utils");
	}

	public static String ts(long ts) {
		return "<span class=\"timestamp\">" + ts + "</span>";
	}

	public static String file(String postId, String filename) {
		return "<a class=\"special\" href=\"javascript:void(0)\" onclick=\"window.open(location.pathname.replace('ot/thread', 'fs')+'/" + postId + "/" + filename + "', '_blank')\">" + filename + "</a>";
	}

	public static String text(String text) {
		if (text == null) {
			return special(null);
		}
		return "<span class=\"special\" title=\"" + text.replaceAll("\n", "\\\\\\\n") + "\">" +
				shortText(text).replaceAll("\\\\n", "<span class=\"newline\">\\\\\\\\n</span>") + "</span>";
	}

	public static String special(@Nullable String text) {
		return text == null ? "<span class=\"null\">null</span>" : "<span class=\"special\">" + text + "</span>";
	}

	public static ObjectDisplayRegistry forEditOperation() {
		return ObjectDisplayRegistry.create()
				.withDisplay(InsertOperation.class,
						($, p) -> "Insert text at position " + p.getPosition(),
						($, p) -> "Insert text '" + text(p.getContent()) + "' at position " + p.getPosition())
				.withDisplay(DeleteOperation.class,
						($, p) -> "Delete text starting from position " + p.getPosition(),
						($, p) -> "Delete text '" + text(p.getContent()) + "' starting from position " + p.getPosition());
	}

	public static ObjectDisplayRegistry forProfileOperation() {
		return ObjectDisplayRegistry.create()
				.withDisplay(new Key<MapOperation<String, String>>() {},
						($, p) -> p.getOperations().isEmpty() ?
								"<empty>" :
								p.getOperations().values().stream()
										.filter(stringSetValue -> !stringSetValue.isEmpty())
										.map(stringSetValue -> (stringSetValue.getPrev() == null ?
												"Add" :
												stringSetValue.getNext() == null ?
														"Remove" :
														"Change") +
												" field")
										.collect(joining("\n")),
						(r, p) -> p.getOperations().isEmpty() ?
								"<empty>" :
								p.getOperations().entrySet().stream()
										.filter(entry -> !entry.getValue().isEmpty())
										.map(entry -> {
											String prev = entry.getValue().getPrev();
											String next = entry.getValue().getNext();
											if (prev == null) {
												assert entry.getValue().getNext() != null;
												return "Add" +
														" field '" + text(entry.getKey()) +
														"' with value '" + text(entry.getValue().getNext()) + '\'';
											}
											if (next == null) {
												return "Remove" +
														" field '" + text(entry.getKey()) +
														"' with value '" + text(entry.getValue().getPrev()) + '\'';
											}
											return "Change" +
													" field '" + text(entry.getKey()) +
													"' from '" + text(entry.getValue().getPrev()) +
													"' to '" + text(entry.getValue().getNext()) + '\'';
										})
										.collect(joining("\n")));
	}

	public static ObjectDisplayRegistry forSharedRepo(String repoType, ObjectDisplayNameProvider nameProvider) {
		return ObjectDisplayRegistry.create()
				.withDisplay(CreateOrDropRepo.class,
						($, p) -> (p.isRemove() ? "Remove" : "Add") + " shared " + repoType,
						(r, p) -> r.getShortDisplay(p) + " " +
								(p.getName().isEmpty() ? "" : ('\'' + text(p.getName()) + "' ")) +
								"with id " + hashId(p.getId()) +
								" and participants: " + p.getParticipants().stream()
								.map(nameProvider::getLongName)
								.collect(joining(", ")))
				.withDisplay(RenameRepo.class,
						($, p) -> "Rename shared " + repoType,
						(r, p) -> r.getShortDisplay(p) +
								" with id " + hashId(p.getId()) +
								" from '" + text(p.getChangeNameOp().getPrev()) +
								"' to '" + text(p.getChangeNameOp().getNext()) + '\'');
	}

	public static ObjectDisplayRegistry forContactsOperation() {
		return ObjectDisplayRegistry.create()
				.withDisplay(ContactsOperation.class,
						($, p) -> (p.isRemove() ? "Remove" : "Add") + " contact",
						(r, p) -> r.getShortDisplay(p) + " with a name '" + text(p.getName()) +
								"' and public key " + pk(p.getPubKey()));
	}

	public static String pk(PubKey pk) {
		return hashId(pk.asString());
	}

	public static String shortHashId(String hashId) {
		return hashId.length() <= 7 ? hashId : hashId.substring(0, 7);
	}

	public static String hashId(String hashId) {
		return "<span class=\"special\" title=\"" + hashId + "\">" + (hashId.length() <= 7 ? hashId : hashId.substring(0, 7)) + "</span>";
	}

	public static String shortText(String text) {
		return (text.length() <= 20 ? text : text.substring(0, 17) + "...").replaceAll("\n", "\\n");
	}

	public interface ObjectDisplayNameProvider {
		String getShortName(PubKey pubKey);

		String getLongName(PubKey pubKey);
	}
}
