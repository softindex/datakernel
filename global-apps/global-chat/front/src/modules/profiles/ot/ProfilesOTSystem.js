import {OTSystemBuilder, TransformResult} from 'ot-core';
import ProfilesOTOperation from './ProfilesOTOperation';

const profilesOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(ProfilesOTOperation, operation => operation.isEmpty())
  .withInvertFunction(ProfilesOTOperation, operation => operation.invert())
  .withSquashFunction(ProfilesOTOperation, ProfilesOTOperation, (operationLeft, operationRight) => {
    if (operationLeft.isEmpty()) {
      return operationRight;
    }

    if (operationRight.isEmpty()) {
      return operationLeft;
    }

    if (operationRight.isEqual(operationLeft.invert())) {
      return ProfilesOTOperation.EMPTY;
    }
  })
  .withTransformFunction(ProfilesOTOperation, ProfilesOTOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })
  .build();

export default profilesOTSystem;
