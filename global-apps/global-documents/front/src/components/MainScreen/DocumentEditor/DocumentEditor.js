import React from 'react';
import {withStyles} from '@material-ui/core';
import {getDifference} from './utils';
import connectService from "../../../common/connectService";
import DocumentContext from "../../../modules/document/DocumentContext";
import editorStyles from "./editorStyles";

class DocumentEditor extends React.Component {
  onContentChange = (event) => {
    const difference = getDifference(this.props.content, event.target.value, event.target.selectionEnd);
    if (!difference) {
      return;
    }

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
      default:
        throw new Error('Unsupported operation');
    }
  };

  render() {
    const {classes} = this.props;
    return (
      <div className={classes.root}>
        <textarea
          className={classes.editor}
          value={this.props.content}
          onChange={this.onContentChange}
        />
      </div>
    );
  }
}

export default connectService(DocumentContext,
  ({content}, documentService) => ({content, documentService})
)(
  withStyles(editorStyles)(DocumentEditor)
);
