import {OTSystemBuilder, TransformResult} from 'ot-core';
import ChatRoomOTOperation from './ChatRoomOTOperation';

const chatRoomOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(ChatRoomOTOperation, operation => operation.isEmpty())
  .withInvertFunction(ChatRoomOTOperation, operation => operation.invert())
  .withSquashFunction(ChatRoomOTOperation, ChatRoomOTOperation, (operationLeft, operationRight) => {
    if (operationLeft.isEmpty()) {
      return operationRight;
    }

    if (operationRight.isEmpty()) {
      return operationLeft;
    }

    if (operationRight.isEqual(operationLeft.invert())) {
      return ChatRoomOTOperation.EMPTY;
    }
  })
  .withTransformFunction(ChatRoomOTOperation, ChatRoomOTOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })
  .build();

export default chatRoomOTSystem;
