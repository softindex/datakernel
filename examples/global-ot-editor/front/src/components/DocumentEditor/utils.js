export function getDifference(oldHtmlValue, nextHtmlValue, caretPosition) {
  const lengthAfterCaret = nextHtmlValue.length - caretPosition;
  const oldCaretPosition = oldHtmlValue.length - lengthAfterCaret;

  if (oldHtmlValue === nextHtmlValue) {
    return null;
  }

  // Detect replace and move caret to end of line
  if (oldHtmlValue.slice(oldCaretPosition) !== nextHtmlValue.slice(caretPosition)) {
    return getDifference(oldHtmlValue, nextHtmlValue, nextHtmlValue.length);
  }

  let i;
  for (i = 0; i < oldCaretPosition; i++) {
    if (oldHtmlValue[i] !== nextHtmlValue[i]) {
      break;
    }
  }

  if (i === oldCaretPosition) {
    if (nextHtmlValue.length > oldHtmlValue.length) {
      const length = nextHtmlValue.length - oldHtmlValue.length;
      const addedHtml = nextHtmlValue.slice(oldCaretPosition, oldCaretPosition + length);
      return {
        operation: 'insert',
        position: getPositionInText(oldHtmlValue, oldCaretPosition),
        content: htmlToText(addedHtml)
      };
    } else {
      const deletedLength = oldHtmlValue.length - nextHtmlValue.length;
      const deletedHtml = oldHtmlValue.slice(
        caretPosition,
        caretPosition + deletedLength
      );
      return {
        operation: 'delete',
        position: getPositionInText(nextHtmlValue, caretPosition),
        content: htmlToText(deletedHtml)
      };
    }
  } else {
    for (i = 0; i < caretPosition; i++) {
      if (oldHtmlValue[i] !== nextHtmlValue[i]) {
        break;
      }
    }

    if (i === caretPosition) {
      const deletedLength = oldHtmlValue.length - nextHtmlValue.length;
      const deletedHtml = oldHtmlValue.slice(
        caretPosition,
        caretPosition + deletedLength
      );
      return {
        operation: 'delete',
        position: getPositionInText(nextHtmlValue, caretPosition),
        content: htmlToText(deletedHtml)
      };
    }
    const deletedLength = oldCaretPosition - i;
    const addedLength = caretPosition - i;
    const deletedHtml = oldHtmlValue.slice(
      i,
      i + deletedLength
    );
    const addedHtml = nextHtmlValue.slice(i, i + addedLength);

    const position = getPositionInText(oldHtmlValue, i);
    return {
      operation: 'replace',
      position,
      oldContent: htmlToText(deletedHtml),
      newContent: htmlToText(addedHtml)
    };
  }
}

function getPositionInText(html, htmlPosition) {
  return htmlToText(html.slice(0, htmlPosition)).length;
}

export function htmlToText(htmlText) {
  const div = document.createElement('div');
  div.innerHTML = htmlText;
  return div.innerText === undefined ? htmlText : div.innerText;
}

export function getPositionInHtml(node, textPosition) {
  const html = getNodeHtml(node);
  const splittedHtml = html.match(/&(#\d+|[^;#&]+);|./g) || [];
  return splittedHtml.slice(0, textPosition).join('').length;
}

export function getNodeHtml(domNode) {
  if (domNode.nodeType !== window.Node.TEXT_NODE) {
    return domNode.outerHTML;
  }

  const div = document.createElement('div');
  div.appendChild(domNode.cloneNode());
  return div.innerHTML;
}

export function getNodeLength(domNode) {
  return getNodeHtml(domNode).length;
}
