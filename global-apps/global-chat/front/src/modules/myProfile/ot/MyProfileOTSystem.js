import {OTSystemBuilder, TransformResult} from 'ot-core';
import MyProfileOTOperation from './MyProfileOTOperation';

const myProfileOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(MyProfileOTOperation, operation => operation.isEmpty())
  .withInvertFunction(MyProfileOTOperation, operation => operation.invert())
  .withSquashFunction(MyProfileOTOperation, MyProfileOTOperation, (operationLeft, operationRight) => {
    if (operationLeft.isEmpty()) {
      return operationRight;
    }

    if (operationRight.isEmpty()) {
      return operationLeft;
    }

    if (operationRight.isEqual(operationLeft.invert())) {
      return MyProfileOTOperation.EMPTY;
    }
  })
  .withTransformFunction(MyProfileOTOperation, MyProfileOTOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })
  .build();

export default myProfileOTSystem;
