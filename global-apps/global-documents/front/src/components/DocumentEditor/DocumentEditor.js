import React, {useEffect} from 'react';
import {Paper, withStyles} from '@material-ui/core';
import {getDifference} from './utils';
import documentEditorStyles from "./documentEditorStyles";
import {getInstance, useService} from "global-apps-common";
import DocumentService from "../../modules/document/DocumentService";
import CircularProgress from "@material-ui/core/CircularProgress";

function DocumentEditorView({classes, onContentChange, content, ready}) {
  let textInput = React.createRef();

  useEffect(() => {
    if (ready) {
      textInput.focus();
    }
  }, [ready]);

  if (!ready) {
    return (
      <CircularProgress
        size={36}
        className={classes.circularProgress}
      />
    )
  }

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
  const {content, ready} = useService(documentService);

  const props = {
    classes,
    content,
    ready,

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

export default withStyles(documentEditorStyles)(DocumentEditor);
