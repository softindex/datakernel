import {OTSystemBuilder, TransformResult} from 'ot-core';
import ContactsOTOperation from './ContactsOTOperation';

const contactsOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(ContactsOTOperation, operation => operation.isEmpty())
  .withInvertFunction(ContactsOTOperation, operation => operation.invert())
  .withSquashFunction(ContactsOTOperation, ContactsOTOperation, (operationLeft, operationRight) => {
    if (operationLeft.isEmpty()) {
      return operationRight;
    }

    if (operationRight.isEmpty()) {
      return operationLeft;
    }

    if (operationRight.isEqual(operationLeft.invert())) {
      return ContactsOTOperation.EMPTY;
    }
  })
  .withTransformFunction(ContactsOTOperation, ContactsOTOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })
  .build();

export default contactsOTSystem;
