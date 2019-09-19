import {OTSystemBuilder, TransformResult} from 'ot-core';
import ChangeNameOperation from "./ChangeNameOperation";

const nameOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(ChangeNameOperation, operation => operation.isEmpty())
  .withInvertFunction(ChangeNameOperation, operation => operation.invert())
  .withTransformFunction(ChangeNameOperation, ChangeNameOperation, (left, right) => {
    if (left.timestamp > right.timestamp)
      return TransformResult.right([new ChangeNameOperation(right.next, left.next, left.timestamp)]);
    if (left.timestamp < right.timestamp)
      return TransformResult.left([new ChangeNameOperation(left.next, right.next, right.timestamp)]);
    if (left.next > right.next)
      return TransformResult.right(new ChangeNameOperation(right.next, left.next, left.timestamp));
    if (left.next < right.next)
      return TransformResult.left(new ChangeNameOperation(left.next, right.next, right.timestamp));
    return TransformResult.empty();
  })
  .withSquashFunction(ChangeNameOperation, ChangeNameOperation, (first, second) =>
    new ChangeNameOperation(first.prev, second.next, second.timestamp))
  .build();

export default nameOTSystem;
