package io.global.forum.ot.session;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.OTSystemImpl.SquashFunction;
import io.datakernel.ot.OTSystemImpl.TransformFunction;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;
import io.global.forum.ot.session.operation.AddOrRemoveSession;
import io.global.forum.ot.session.operation.SessionOperation;
import io.global.forum.ot.session.operation.UpdateTimestamp;

import static io.global.forum.ot.session.operation.AddOrRemoveSession.add;
import static io.global.forum.ot.session.operation.AddOrRemoveSession.remove;
import static java.util.Collections.singletonList;

public final class SessionOTSystem {
	public static final OTSystem<SessionOperation> SYSTEM = create();

	private static OTSystem<SessionOperation> create() {
		return OTSystemImpl.<SessionOperation>create()
				.withEmptyPredicate(AddOrRemoveSession.class, op -> op.getTimestamp() == -1)
				.withEmptyPredicate(UpdateTimestamp.class, op -> op.getPrevious() == op.getNext())

				.withInvertFunction(AddOrRemoveSession.class, op -> singletonList(op.invert()))
				.withInvertFunction(UpdateTimestamp.class, op -> singletonList(UpdateTimestamp.update(op.getSessionId(), op.getNext(), op.getPrevious())))

				.withSquashFunction(AddOrRemoveSession.class, AddOrRemoveSession.class, squashSameId(($1, $2) ->
						AddOrRemoveSession.EMPTY))
				.withSquashFunction(UpdateTimestamp.class, UpdateTimestamp.class, squashSameId((first, second) ->
						UpdateTimestamp.update(first.getSessionId(), first.getPrevious(), second.getNext())))
				.withSquashFunction(AddOrRemoveSession.class, UpdateTimestamp.class, squashSameId((first, second) ->
						add(first.getSessionId(), first.getUserId(), second.getNext())))
				.withSquashFunction(UpdateTimestamp.class, AddOrRemoveSession.class, squashSameId((first, second) ->
						remove(first.getSessionId(), second.getUserId(), first.getPrevious())))

				.withTransformFunction(AddOrRemoveSession.class, AddOrRemoveSession.class, transformSameId(((left, right) -> {
							if (!left.equals(right)) {
								throw new OTTransformException("ID collision");
							}
							return TransformResult.empty();
						})
				))
				.withTransformFunction(UpdateTimestamp.class, UpdateTimestamp.class, transformSameId(((left, right) -> {
							long leftNext = left.getNext();
							long rightNext = right.getNext();
							if (leftNext > rightNext) {
								return TransformResult.right(UpdateTimestamp.update(left.getSessionId(), rightNext, leftNext));
							} else if (leftNext < rightNext) {
								return TransformResult.left(UpdateTimestamp.update(left.getSessionId(), leftNext, rightNext));
							}
							return TransformResult.empty();
						})
				))
				.withTransformFunction(AddOrRemoveSession.class, UpdateTimestamp.class, transformSameId(((left, right) -> {
							if (!left.isRemove()) {
								throw new OTTransformException("ID collision");
							}
							return TransformResult.right(remove(left.getSessionId(), left.getUserId(), right.getNext()));
						})
				));
	}

	private static <OP1 extends SessionOperation, OP2 extends SessionOperation> SquashFunction<SessionOperation, OP1, OP2>
	squashSameId(SquashFunction<SessionOperation, OP1, OP2> next) {
		return (first, second) -> {
			if (!first.getSessionId().equals(second.getSessionId())) {
				return null;
			}

			return next.trySquash(first, second);
		};
	}

	private static <OP1 extends SessionOperation, OP2 extends SessionOperation> TransformFunction<SessionOperation, OP1, OP2>
	transformSameId(TransformFunction<SessionOperation, OP1, OP2> next) {
		return (left, right) -> {
			if (!left.getSessionId().equals(right.getSessionId())) {
				return TransformResult.of(right, left);
			}
			return next.transform(left, right);
		};
	}

}
