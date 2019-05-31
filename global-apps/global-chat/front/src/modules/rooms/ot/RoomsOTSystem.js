import {OTSystemBuilder, TransformResult} from 'ot-core';
import RoomsOTOperation from './RoomsOTOperation';

const roomsOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(RoomsOTOperation, operation => operation.isEmpty())
  .withInvertFunction(RoomsOTOperation, operation => operation.invert())
  .withSquashFunction(RoomsOTOperation, RoomsOTOperation, (operationLeft, operationRight) => {
    if (operationLeft.isEmpty()) {
      return operationRight;
    }

    if (operationRight.isEmpty()) {
      return operationLeft;
    }

    if (operationRight.isEqual(operationLeft.invert())) {
      return RoomsOTOperation.EMPTY;
    }
  })
  .withTransformFunction(RoomsOTOperation, RoomsOTOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })
  .build();

export default roomsOTSystem;
