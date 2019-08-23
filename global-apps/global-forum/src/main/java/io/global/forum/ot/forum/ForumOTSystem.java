package io.global.forum.ot.forum;

import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.global.forum.pojo.Thread;
import io.global.ot.map.MapOTSystem;
import io.global.ot.name.ChangeName;
import io.global.ot.name.NameOTSystem;

import java.util.Comparator;

public final class ForumOTSystem {
	private static final OTSystem<ChangeName> NAME_OT_SYSTEM = NameOTSystem.createOTSystem();

	public static OTSystem<ForumOTOperation> create(Comparator<Thread> comparator) {
		return MergedOTSystem.mergeOtSystems(ForumOTOperation::new,
				ForumOTOperation::getNameOps, NAME_OT_SYSTEM,
				ForumOTOperation::getDescriptionOps, NAME_OT_SYSTEM,
				ForumOTOperation::getMetadataOps, MapOTSystem.create(comparator));
	}
}
