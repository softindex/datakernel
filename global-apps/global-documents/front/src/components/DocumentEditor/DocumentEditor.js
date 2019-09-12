import React, {useEffect} from 'react';
import {Paper, withStyles} from '@material-ui/core';
import {getDifference} from './utils';
import editorStyles from "./editorStyles";
import {getInstance, useService} from "global-apps-common";
import DocumentService from "../../modules/document/DocumentService";

function DocumentEditorView({classes, onContentChange, content}) {
  let textInput = React.createRef();

  useEffect(() => {
    textInput.focus();
  }, [textInput]);

  return (
    <Paper className={classes.paper}>
      <textarea
        className={classes.editor}
        value={content}
        onChange={onContentChange}
        ref={input => textInput = input}
      />
    </Paper>
  );
}

function DocumentEditor({classes, onInsert, onDelete, onReplace}) {
  const documentService = getInstance(DocumentService);
  const {content} = useService(documentService);

  const props = {
    classes,
    content,

    onContentChange(event) {
      const difference = getDifference(content, event.target.value, event.target.selectionEnd);
      if (!difference) {
        return;
      }

      switch (difference.operation) {
        case 'insert':
          onInsert(difference.position, difference.content);
          break;
        case 'delete':
          onDelete(difference.position, difference.content);
          break;
        case 'replace':
          onReplace(difference.position, difference.oldContent, difference.newContent);
          break;
        default:
          throw new Error('Unsupported operation');
      }
    }
  };

  return <DocumentEditorView {...props}/>
}

export default withStyles(editorStyles)(DocumentEditor);
