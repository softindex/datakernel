package io.global.ot.shared;

import io.global.common.PubKey;

import java.util.Map;
import java.util.Set;

import static io.datakernel.util.CollectionUtils.map;

public class CreateOrDropRepos implements SharedReposOperation {
	private final Map<String, RepoInfo> repoInfos;

	public CreateOrDropRepos(Map<String, RepoInfo> repoInfos) {
		this.repoInfos = repoInfos;
	}

	public static CreateOrDropRepos of(String id, RepoInfo repoInfo) {
		return new CreateOrDropRepos(map(id, repoInfo));
	}

	public static CreateOrDropRepos create(String id, String name, Set<PubKey> participants) {
		return of(id, new RepoInfo(name, participants, false));
	}

	public static CreateOrDropRepos drop(String id, String name, Set<PubKey> participants) {
		return of(id, new RepoInfo(name, participants, true));
	}

	@Override
	public void apply(Map<String, SharedRepo> repos) {
		repoInfos.forEach((key, value) -> {
			if (value.isRemove()) {
				repos.remove(key);
			} else {
				repos.put(key, new SharedRepo(value.getName(), value.getParticipants()));
			}
		});
	}

	public Map<String, RepoInfo> getRepoInfos() {
		return repoInfos;
	}
}
