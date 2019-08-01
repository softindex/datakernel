import {OTSystemBuilder, TransformResult} from 'ot-core';
import ProfileOTOperation from './ProfileOTOperation';

const profileOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(ProfileOTOperation, operation => operation.isEmpty())
  .withInvertFunction(ProfileOTOperation, operation => operation.invert())
  .withSquashFunction(ProfileOTOperation, ProfileOTOperation, (operationLeft, operationRight) => {
    if (operationLeft.isEmpty()) {
      return operationRight;
    }

    if (operationRight.isEmpty()) {
      return operationLeft;
    }

    if (operationRight.isEqual(operationLeft.invert())) {
      return ProfileOTOperation.EMPTY;
    }
  })
  .withTransformFunction(ProfileOTOperation, ProfileOTOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })
  .build();

export default profileOTSystem;
