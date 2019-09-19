package io.global.ot.shared;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.TransformResult;
import io.global.ot.name.ChangeName;
import io.global.ot.name.NameOTSystem;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static io.global.ot.shared.CreateOrDropRepo.drop;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public final class SharedReposOTSystem {
	private static final OTSystem<ChangeName> CHANGE_NAME_OT_SYSTEM = NameOTSystem.createOTSystem();

	private SharedReposOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<SharedReposOperation> createOTSystem() {
		return OTSystemImpl.<SharedReposOperation>create()
				.withEmptyPredicate(CreateOrDropRepo.class, CreateOrDropRepo::isEmpty)
				.withEmptyPredicate(RenameRepo.class, RenameRepo::isEmpty)

				.withInvertFunction(CreateOrDropRepo.class, op -> singletonList(op.invert()))
				.withInvertFunction(RenameRepo.class, op -> singletonList(op.invert()))

				.withTransformFunction(CreateOrDropRepo.class, CreateOrDropRepo.class, (left, right) -> TransformResult.of(right, left))
				.withTransformFunction(RenameRepo.class, RenameRepo.class, (left, right) -> {
					if (!left.getId().equals(right.getId())) {
						return TransformResult.of(right, left);
					}
					String id = left.getId();
					TransformResult<ChangeName> result = CHANGE_NAME_OT_SYSTEM
							.transform(left.getChangeNameOp(), right.getChangeNameOp());
					return TransformResult.of(
							collectDiffs(id, result.left),
							collectDiffs(id, result.right)
					);
				})
				.withTransformFunction(CreateOrDropRepo.class, RenameRepo.class, (left, right) -> {
					if (!left.getId().equals(right.getId())) {
						return TransformResult.of(right, left);
					}

					String id = left.getId();
					// Remove wins
					ChangeName changeNameOp = right.getChangeNameOp();
					if (left.isRemove()) {
						return TransformResult.right(drop(new SharedRepo(
								id,
								changeNameOp.getNext(),
								left.getParticipants()
						)));
					}

					return TransformResult.left(RenameRepo.of(
							id,
							left.getName(),
							changeNameOp.getNext(),
							changeNameOp.getTimestamp()));
				})

				.withSquashFunction(CreateOrDropRepo.class, CreateOrDropRepo.class, (op1, op2) -> {
					if (op1.isEmpty()) return op2;
					if (op2.isEmpty()) return op1;
					if (op1.isInversionFor(op2)) {
						return CreateOrDropRepo.EMPTY;
					}
					return null;
				})
				.withSquashFunction(RenameRepo.class, RenameRepo.class, (first, second) -> {
					if (!first.getId().equals(second.getId())) {
						return null;
					}
					return RenameRepo.of(
							first.getId(),
							first.getChangeNameOp().getPrev(),
							second.getChangeNameOp().getNext(),
							second.getChangeNameOp().getTimestamp()
					);
				})
				.withSquashFunction(CreateOrDropRepo.class, RenameRepo.class, (first, second) -> {
					if (!first.getId().equals(second.getId())) {
						return null;
					}
					return first.isRemove() ?
							first :
							CreateOrDropRepo.create(
									new SharedRepo(
											first.getId(),
											second.getChangeNameOp().getNext(),
											first.getParticipants()
									)
							);
				})
				.withSquashFunction(RenameRepo.class, CreateOrDropRepo.class, (first, second) -> {
					if (!first.getId().equals(second.getId())) {
						return null;
					}
					return second.isRemove() ?
							CreateOrDropRepo.drop(
									new SharedRepo(
											first.getId(),
											first.getChangeNameOp().getPrev(),
											second.getParticipants()
									)
							) :
							null;

				});
	}

	@NotNull
	public static List<RenameRepo> collectDiffs(String id, List<ChangeName> diffs) {
		return diffs.stream()
				.map(changeName -> RenameRepo.of(id, changeName))
				.collect(toList());
	}

}
