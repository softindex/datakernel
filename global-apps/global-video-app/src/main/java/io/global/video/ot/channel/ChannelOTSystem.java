package io.global.video.ot.channel;

import io.datakernel.ot.MergedOTSystem;
import io.datakernel.ot.OTSystem;
import io.global.ot.map.MapOTSystem;
import io.global.ot.name.ChangeName;
import io.global.ot.name.NameOTSystem;
import io.global.video.pojo.VideoMetadata;

import java.util.Comparator;

public final class ChannelOTSystem {
	private static final OTSystem<ChangeName> NAME_OT_SYSTEM = NameOTSystem.createOTSystem();

	public static OTSystem<ChannelOTOperation> create(Comparator<VideoMetadata> comparator) {
		return MergedOTSystem.mergeOtSystems(ChannelOTOperation::new,
				ChannelOTOperation::getNameOps, NAME_OT_SYSTEM,
				ChannelOTOperation::getDescriptionOps, NAME_OT_SYSTEM,
				ChannelOTOperation::getMetadataOps, MapOTSystem.createOTSystem(comparator));
	}
}
