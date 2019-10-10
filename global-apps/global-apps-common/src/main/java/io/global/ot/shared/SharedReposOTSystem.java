package io.global.ot.shared;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;
import io.global.ot.map.MapOTSystem;
import io.global.ot.map.MapOperation;
import io.global.ot.map.SetValue;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.CollectionUtils.intersection;
import static io.datakernel.util.CollectorsEx.toMap;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class SharedReposOTSystem {
	private static final OTSystem<MapOperation<String, String>> RENAME_REPO_SUBSYSTEM = MapOTSystem.create();

	private SharedReposOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<SharedReposOperation> createOTSystem() {
		return OTSystemImpl.<SharedReposOperation>create()
				.withEmptyPredicate(CreateOrDropRepos.class, createOrDropRepos -> createOrDropRepos.getRepoInfos().values().stream()
						.allMatch(repoInfo -> repoInfo.getParticipants().isEmpty()))
				.withEmptyPredicate(RenameRepos.class, renameRepos -> RENAME_REPO_SUBSYSTEM.isEmpty(renameRepos.getRenames()))

				.withInvertFunction(CreateOrDropRepos.class, op -> singletonList(
						new CreateOrDropRepos(op.getRepoInfos().entrySet().stream()
								.collect(toMap(Entry::getKey, e -> {
									RepoInfo repoInfo = e.getValue();
									return new RepoInfo(repoInfo.getName(), repoInfo.getParticipants(), !repoInfo.isRemove());
								})))))
				.withInvertFunction(RenameRepos.class, op -> RENAME_REPO_SUBSYSTEM.invert(singletonList(op.getRenames()))
						.stream()
						.map(RenameRepos::new)
						.collect(toList()))

				.withTransformFunction(CreateOrDropRepos.class, CreateOrDropRepos.class, (left, right) -> {
					Map<String, RepoInfo> leftRepos = left.getRepoInfos();
					Map<String, RepoInfo> rightRepos = right.getRepoInfos();

					Set<String> intersection = intersection(leftRepos.keySet(), rightRepos.keySet());
					if (intersection.isEmpty()) return TransformResult.of(right, left);

					List<SharedReposOperation> leftTransformed = new ArrayList<>();
					List<SharedReposOperation> rightTransformed = new ArrayList<>();

					Map<String, RepoInfo> diffLeft = difference(rightRepos, intersection);
					Map<String, RepoInfo> diffRight = difference(leftRepos, intersection);
					if (!diffLeft.isEmpty()) leftTransformed.add(new CreateOrDropRepos(diffLeft));
					if (!diffRight.isEmpty()) rightTransformed.add(new CreateOrDropRepos(diffRight));

					for (String key : intersection) {
						RepoInfo leftRepo = leftRepos.get(key);
						RepoInfo rightRepo = rightRepos.get(key);
						if (leftRepo.equals(rightRepo)) {
							continue;
						}

						if (leftRepo.isRemove() || rightRepo.isRemove()) {
							throw new OTTransformException("If any operation is 'remove', both operations should be equal");
						}

						// 2 adds with the same ID -> the one with more participants wins

						if (leftRepo.getParticipants().size() > rightRepo.getParticipants().size()) {
							RepoInfo inverted = new RepoInfo(rightRepo.getName(), rightRepo.getParticipants(), !rightRepo.isRemove());
							rightTransformed.add(CreateOrDropRepos.of(key, inverted));
							rightTransformed.add(CreateOrDropRepos.of(key, leftRepo));
						} else {
							RepoInfo inverted = new RepoInfo(leftRepo.getName(), leftRepo.getParticipants(), !leftRepo.isRemove());
							leftTransformed.add(CreateOrDropRepos.of(key, inverted));
							leftTransformed.add(CreateOrDropRepos.of(key, rightRepo));
						}
					}

					return TransformResult.of(leftTransformed, rightTransformed);
				})
				.withTransformFunction(RenameRepos.class, RenameRepos.class, (left, right) -> {
					TransformResult<MapOperation<String, String>> transform = RENAME_REPO_SUBSYSTEM
							.transform(left.getRenames(), right.getRenames());
					return TransformResult.of(
							transform.left.stream().map(RenameRepos::new).collect(toList()),
							transform.right.stream().map(RenameRepos::new).collect(toList())
					);
				})
				.withTransformFunction(CreateOrDropRepos.class, RenameRepos.class, (left, right) -> {
					Map<String, RepoInfo> repos = left.getRepoInfos();
					Map<String, SetValue<String>> setValues = right.getRenames().getOperations();

					Set<String> intersection = intersection(repos.keySet(), setValues.keySet());
					if (intersection.isEmpty()) return TransformResult.of(right, left);

					List<SharedReposOperation> leftTransformed = new ArrayList<>();
					List<SharedReposOperation> rightTransformed = new ArrayList<>();

					Map<String, SetValue<String>> diffLeft = difference(setValues, intersection);
					Map<String, RepoInfo> diffRight = difference(repos, intersection);
					if (!diffLeft.isEmpty()) leftTransformed.add(new RenameRepos(MapOperation.of(diffLeft)));
					if (!diffRight.isEmpty()) rightTransformed.add(new CreateOrDropRepos(diffRight));

					Map<String, RepoInfo> repoInfos = new HashMap<>();
					for (String key : intersection) {
						RepoInfo repo = repos.get(key);
						String nextName = setValues.get(key).getNext();
						assert nextName != null;
						if (!repo.isRemove()) {
							throw new OTTransformException("Invalid operation");
						}

						// Remove wins
						repoInfos.put(key, new RepoInfo(nextName, repo.getParticipants(), true));
					}
					rightTransformed.add(new CreateOrDropRepos(repoInfos));

					return TransformResult.of(leftTransformed, rightTransformed);
				})

				.withSquashFunction(CreateOrDropRepos.class, CreateOrDropRepos.class, (op1, op2) -> {
					Map<String, RepoInfo> repos1 = op1.getRepoInfos();
					Map<String, RepoInfo> repos2 = op2.getRepoInfos();
					Set<String> intersection = intersection(repos1.keySet(), repos2.keySet());

					return new CreateOrDropRepos(Stream
							.concat(repos1.entrySet().stream(), repos2.entrySet().stream())
							.filter(sharedRepo -> !intersection.contains(sharedRepo.getKey()))
							.collect(toMap()));
				})
				.withSquashFunction(RenameRepos.class, RenameRepos.class, (first, second) ->
						new RenameRepos(first(RENAME_REPO_SUBSYSTEM.squash(asList(first.getRenames(), second.getRenames())))))
				.withSquashFunction(CreateOrDropRepos.class, RenameRepos.class, (first, second) -> doSquash(second, first))
				.withSquashFunction(RenameRepos.class, CreateOrDropRepos.class, SharedReposOTSystem::doSquash);
	}

	private static <T> Map<String, T> difference(Map<String, T> repos, Set<String> intersection) {
		return repos.entrySet()
				.stream()
				.filter(entry -> !intersection.contains(entry.getKey()))
				.collect(toMap());
	}

	@Nullable
	private static SharedReposOperation doSquash(RenameRepos rename, CreateOrDropRepos createOrDrop) {
		Map<String, SetValue<String>> renameOps = rename.getRenames().getOperations();
		Map<String, RepoInfo> repoInfos = createOrDrop.getRepoInfos();
		if (!renameOps.keySet().containsAll(repoInfos.keySet())) {
			return null;
		}
		return new CreateOrDropRepos(repoInfos.entrySet()
				.stream()
				.collect(toMap(Entry::getKey, e -> {
					RepoInfo prev = e.getValue();
					SetValue<String> setValue = renameOps.get(e.getKey());
					if (setValue == null || setValue.isEmpty()) {
						return prev;
					}
					assert setValue.getNext() != null;
					return new RepoInfo(setValue.getNext(), prev.getParticipants(), prev.isRemove());
				})));

	}

}
