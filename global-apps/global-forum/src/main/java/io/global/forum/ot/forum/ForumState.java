package io.global.forum.ot.forum;

import io.datakernel.ot.OTState;
import io.global.forum.pojo.ThreadMetadata;
import io.global.ot.map.MapOTState;
import io.global.ot.name.ChangeName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class ForumState implements OTState<ForumOTOperation> {
	private String name;
	private String description;
	private final MapOTState<String, ThreadMetadata> metadata = new MapOTState<>(new LinkedHashMap<>());
	private Consumer<ForumOTOperation> listener;

	public void setListener(Consumer<ForumOTOperation> listener) {
		this.listener = listener;
	}

	@Override
	public void init() {
		name = "";
		description = "";
		metadata.init();
	}

	@Override
	public void apply(ForumOTOperation op) {
		applyStringOps(op.getNameOps(), name -> this.name = name);
		applyStringOps(op.getDescriptionOps(), description -> this.description = description);
		op.getMetadataOps().forEach(metadata::apply);

		if (listener != null) {
			listener.accept(op);
		}
	}

	public void applyStringOps(List<ChangeName> ops, Consumer<String> consumer) {
		int nameOpsSize = ops.size();
		if (nameOpsSize != 0) {
			consumer.accept(ops.get(nameOpsSize - 1).getNext());
		}
	}

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	public Map<String, ThreadMetadata> getMetadata() {
		return metadata.getMap();
	}
}
