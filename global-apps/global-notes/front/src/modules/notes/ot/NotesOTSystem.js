import {OTSystemBuilder, TransformResult} from 'ot-core';
import NotesOTOperation from './NotesOTOperation';

const notesOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(NotesOTOperation, operation => operation.isEmpty())
  .withInvertFunction(NotesOTOperation, operation => operation.invert())
  .withSquashFunction(NotesOTOperation, NotesOTOperation, (operationLeft, operationRight) => {
    if (operationLeft.isEmpty()) {
      return operationRight;
    }

    if (operationRight.isEmpty()) {
      return operationLeft;
    }

    if (operationRight.isEqual(operationLeft.invert())) {
      return NotesOTOperation.EMPTY;
    }
  })
  .withTransformFunction(NotesOTOperation, NotesOTOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })
  .build();

export default notesOTSystem;
