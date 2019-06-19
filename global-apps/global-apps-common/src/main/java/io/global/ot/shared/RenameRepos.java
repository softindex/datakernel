package io.global.ot.shared;

import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;

import java.util.Map;

public final class RenameRepos implements SharedReposOperation {
	private final MapOperation<String, String> renames;

	public RenameRepos(MapOperation<String, String> renames) {
		this.renames = renames;
	}

	public static RenameRepos of(String id, String prev, String next) {
		return new RenameRepos(MapOperation.forKey(id, SetValue.set(prev, next)));
	}

	@Override
	public void apply(Map<String, SharedRepo> repos) {
		//noinspection ConstantConditions - Cannot be null
		renames.getOperations().forEach((id, setValue) -> repos.get(id).setName(setValue.getNext()));
	}

	public MapOperation<String, String> getRenames() {
		return renames;
	}
}
