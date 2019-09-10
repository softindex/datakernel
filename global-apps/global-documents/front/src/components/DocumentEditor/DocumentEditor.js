import React, {useEffect} from 'react';
import {Paper, withStyles} from '@material-ui/core';
import {getDifference} from './utils';
import {connectService} from "global-apps-common";
import DocumentContext from "../../modules/document/DocumentContext";
import editorStyles from "./editorStyles";

function DocumentEditor(props) {
  let textInput = React.createRef();

  useEffect(() => {
    textInput.focus();
  }, [textInput]);

  const onContentChange = event => {
    const difference = getDifference(props.content, event.target.value, event.target.selectionEnd);
    if (!difference) {
      return;
    }

    switch (difference.operation) {
      case 'insert':
        props.onInsert(difference.position, difference.content);
        break;
      case 'delete':
        props.onDelete(difference.position, difference.content);
        break;
      case 'replace':
        props.onReplace(difference.position, difference.oldContent, difference.newContent);
        break;
      default:
        throw new Error('Unsupported operation');
    }
  };

  return (
    <Paper className={props.classes.paper}>
      <textarea
        className={props.classes.editor}
        value={props.content}
        onChange={onContentChange}
        ref={input => textInput = input}
      />
    </Paper>
  );
}

export default connectService(DocumentContext,
  ({content}, documentService) => ({content, documentService})
)(
  withStyles(editorStyles)(DocumentEditor)
);
