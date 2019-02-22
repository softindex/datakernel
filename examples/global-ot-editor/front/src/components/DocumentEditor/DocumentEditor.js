import React from "react";
import {
  getDifference,
  htmlToText,
  getNodeLength,
  getPositionInHtml,
  createOperations
} from "./utils";
import "./styles/index.css";

class TextArea extends React.Component {
  componentDidMount() {
    this.renderState(this.props.value);
  }

  componentWillReceiveProps(nextProps) {
    const nextOtState = nextProps.value;
    if (nextOtState !== htmlToText(this.domElement.innerHTML)) {
      this.renderState(nextOtState);
    }
  }

  renderState(otState) {
    this.domElement.innerHTML = this.prevHtml = otState.replace("\n", "<br/>");
  }

  getSiblingsNodesLength(htmlElement) {
    if (htmlElement === this.domElement) {
      return 0;
    }
    var length = htmlElement.tagName ? 2 + htmlElement.tagName.length : 0;
    for (var i = 0; i < htmlElement.parentElement.childNodes.length; i++) {
      var a = htmlElement.parentElement.childNodes[i];
      if (a === htmlElement) {
        break;
      }
      length += getNodeLength(a);
    }
    return length + this.getSiblingsNodesLength(htmlElement.parentElement);
  }

  getCaretPosition() {
    var selection = window.getSelection();
    const node = selection.anchorNode;
    var offset = selection.getRangeAt(0).endOffset;
    let pos = 0;
    if (node.nodeType === window.Node.TEXT_NODE) {
      pos = getPositionInHtml(node, offset);
    } else {
      for (let i = 0; i < offset; i++) {
        const currentNode = node.childNodes[i];
        pos += getNodeLength(currentNode);
      }
    }

    return pos + this.getSiblingsNodesLength(node);
  }

  inputHandler(e) {
    const pos = this.getCaretPosition();
    const difference = getDifference(this.prevHtml, e.target.innerHTML, pos);
    if (!difference) {
      return;
    }

    this.prevHtml = e.target.innerHTML;

    switch (difference.operation) {
      case 'insert':
        this.props.onInsert(difference.position, difference.content);
        break;
      case 'delete':
        this.props.onDelete(difference.position, difference.content);
        break;
      case 'replace':
        this.props.onReplace(difference.position, difference.oldContent, difference.newContent);
        break;
    }
  }

  render() {
    return (
      <div className="document-editor">
        <div className="wrapper">
          <div
            className="document-form"
            ref={ref => {
              this.domElement = ref;
            }}
            contentEditable={true}
            onInput={this.inputHandler.bind(this)}
          />
        </div>
      </div>
    );
  }
}
export default TextArea;
