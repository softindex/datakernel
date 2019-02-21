import React from "react";
import {
  getStringDifference,
  htmlToText,
  getNodeLength,
  getPositionInHtml,
  createOperations
} from "./utils";
import "./styles/index.css";

class TextArea extends React.Component {
  componentDidMount() {
    this.renderOTState(
      this.applyChanges(this.props.value.changes, this.props.value.initValue)
    );
  }

  componentWillReceiveProps(nextProps) {
    const nextOtState = this.applyChanges(
      nextProps.value.changes,
      nextProps.value.initValue
    );
    if (nextOtState !== htmlToText(this.domElement.innerHTML)) {
      this.renderOTState(nextOtState);
    }
  }

  renderOTState(otState) {
    this.domElement.innerHTML = this.prevHtml = otState.replace("\n", "<br/>");
  }

  applyChanges(operations, value) {
    return operations.reduce((accumulator, operation) => {
      return operation.apply(accumulator);
    }, value);
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
    let pos = this.getCaretPosition();
    const diffs = getStringDifference(this.prevHtml, e.target.innerHTML, pos);
    const ops = createOperations(diffs, this.prevHtml, e.target.innerHTML);
    this.prevHtml = e.target.innerHTML;
    const value = {
      initValue: this.props.value.initValue,
      changes: [...this.props.value.changes, ...ops]
    };
    this.props.onChange(value);
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
