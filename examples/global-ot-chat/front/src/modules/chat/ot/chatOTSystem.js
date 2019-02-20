import {OTSystemBuilder, TransformResult} from 'ot-core';
import ChatOTOperation from './ChatOTOperation';

const chatOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(ChatOTOperation, operation => operation.isEmpty())
  .withInvertFunction(ChatOTOperation, operation => operation.invert())
  .withSquashFunction(ChatOTOperation, ChatOTOperation, (operationLeft, operationRight) => {
    if (operationLeft.isEmpty()) {
      return operationRight;
    }

    if (operationRight.isEmpty()) {
      return operationLeft;
    }

    if (operationRight.isEqual(operationLeft.invert())) {
      return ChatOTOperation.EMPTY;
    }
  })
  .withTransformFunction(ChatOTOperation, ChatOTOperation, (operationLeft, operationRight) => {
    return TransformResult.of(operationRight, operationLeft);
  })
  .build();

export default chatOTSystem;
