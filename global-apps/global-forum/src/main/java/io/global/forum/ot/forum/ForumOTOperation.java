package io.global.forum.ot.forum;

import io.global.forum.pojo.Thread;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import io.global.ot.name.ChangeName;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class ForumOTOperation {
	private final List<ChangeName> nameOps;
	private final List<ChangeName> descriptionOps;
	private final List<MapOperation<String, Thread>> metadataOps;

	public ForumOTOperation(List<ChangeName> nameOps,
			List<ChangeName> descriptionOps,
			List<MapOperation<String, Thread>> metadataOps) {
		this.nameOps = nameOps;
		this.descriptionOps = descriptionOps;
		this.metadataOps = metadataOps;
	}

	public static ForumOTOperation changeName(String previous, String next, long timestamp) {
		return new ForumOTOperation(singletonList(new ChangeName(previous, next, timestamp)), emptyList(), emptyList());
	}

	public static ForumOTOperation changeDescription(String previous, String next, long timestamp) {
		return new ForumOTOperation(emptyList(), singletonList(new ChangeName(previous, next, timestamp)), emptyList());
	}

	public static ForumOTOperation setMetadata(String id, @Nullable Thread previous, @Nullable Thread next) {
		MapOperation<String, Thread> mapOperation = MapOperation.forKey(id, SetValue.set(previous, next));
		return new ForumOTOperation(emptyList(), emptyList(), singletonList(mapOperation));
	}

	public List<ChangeName> getNameOps() {
		return nameOps;
	}

	public List<ChangeName> getDescriptionOps() {
		return descriptionOps;
	}

	public List<MapOperation<String, Thread>> getMetadataOps() {
		return metadataOps;
	}
}
