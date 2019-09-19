export function getDifference(oldValue, nextValue, caretPosition) {
  const lengthAfterCaret = nextValue.length - caretPosition;
  const oldCaretPosition = oldValue.length - lengthAfterCaret;

  if (oldValue === nextValue) {
    return null;
  }

  // Detect replace and move caret to end of line
  if (oldValue.slice(oldCaretPosition) !== nextValue.slice(caretPosition)) {
    return getDifference(oldValue, nextValue, nextValue.length);
  }

  let i;
  for (i = 0; i < oldCaretPosition; i++) {
    if (oldValue[i] !== nextValue[i]) {
      break;
    }
  }

  if (i === oldCaretPosition) {
    if (nextValue.length > oldValue.length) {
      const length = nextValue.length - oldValue.length;
      const addedHtml = nextValue.slice(oldCaretPosition, oldCaretPosition + length);
      return {
        operation: 'insert',
        position: oldCaretPosition,
        content: addedHtml
      };
    } else {
      const deletedLength = oldValue.length - nextValue.length;
      const deletedHtml = oldValue.slice(caretPosition, caretPosition + deletedLength);
      return {
        operation: 'delete',
        position: caretPosition,
        content: deletedHtml
      };
    }
  } else {
    for (i = 0; i < caretPosition; i++) {
      if (oldValue[i] !== nextValue[i]) {
        break;
      }
    }

    if (i === caretPosition) {
      const deletedLength = oldValue.length - nextValue.length;
      const deletedHtml = oldValue.slice(caretPosition, caretPosition + deletedLength);
      return {
        operation: 'delete',
        position: caretPosition,
        content: deletedHtml
      };
    }
    const deletedLength = oldCaretPosition - i;
    const addedLength = caretPosition - i;
    const deletedHtml = oldValue.slice(i, i + deletedLength);
    const addedHtml = nextValue.slice(i, i + addedLength);

    return {
      operation: 'replace',
      position: i,
      oldContent: deletedHtml,
      newContent: addedHtml
    };
  }
}
