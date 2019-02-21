import InsertOperation from "./operations/InsertOperation";
import DeleteOperation from "./operations/DeleteOperation";

export function getStringDifference(str1, str2, caretPosition) {
  const lengthAfterCaret = str2.length - caretPosition;
  const oldCaretPosition = str1.length - lengthAfterCaret;

  // console.log(str1, str2, caretPosition);
  if (str1 === str2) {
    return [];
  }

  if (str1.slice(oldCaretPosition) !== str2.slice(caretPosition)) {
    return getStringDifference(str1, str2, str2.length);
  }

  let i;
  for (i = 0; i < oldCaretPosition; i++) {
    if (str1[i] !== str2[i]) {
      break;
    }
  }

  if (i === oldCaretPosition) {
    if (str2.length > str1.length) {
      return [
        {
          type: "insert",
          start: oldCaretPosition,
          length: str2.length - str1.length
        }
      ];
    } else {
      return [
        {
          type: "delete",
          start: caretPosition,
          length: str1.length - str2.length
        }
      ];
    }
  } else {
    for (i = 0; i < caretPosition; i++) {
      if (str1[i] !== str2[i]) {
        break;
      }
    }
    if (i === caretPosition) {
      return [
        {
          type: "delete",
          start: caretPosition,
          length: str1.length - str2.length
        }
      ];
    }
    return [
      {
        type: "delete",
        start: i,
        length: oldCaretPosition - i
      },
      {
        type: "insert",
        start: i,
        length: caretPosition - i
      }
    ];
  }
}

export function htmlToText(htmlText) {
  const div = document.createElement("div");
  div.innerHTML = htmlText;
  return div.innerText;
}

export function getPositionInHtml(node, textPosition) {
  const html = getNodeHtml(node);
  const splittedHtml = html.match(/&(#\d+|[^;#&]+);|./g) || [];
  return splittedHtml.slice(0, textPosition).join("").length;
}

export function getNodeHtml(domNode) {
  if (domNode.nodeType !== window.Node.TEXT_NODE) {
    return domNode.outerHTML;
  }

  const div = document.createElement("div");
  div.appendChild(domNode.cloneNode());
  return div.innerHTML;
}

export function getNodeLength(domNode) {
  return getNodeHtml(domNode).length;
}

export function createOperations(diffs, oldHtmlValue, nextHtmlValue) {
  return diffs.map(diff => {
    const positionInText = htmlToText(nextHtmlValue.slice(0, diff.start))
      .length;
    switch (diff.type) {
      case "insert":
        const content = htmlToText(
          nextHtmlValue.slice(diff.start, diff.start + diff.length)
        );
        return new InsertOperation(positionInText, content);
      case "delete":
        const deletedHtml = oldHtmlValue.slice(
          diff.start,
          diff.start + diff.length
        );
        return new DeleteOperation(
          positionInText,
          htmlToText(deletedHtml)
        );
      default:
        throw new Error("Unknown operation");
    }
  });
}
