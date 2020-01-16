import React, {useEffect} from 'react';
import {getDifference} from './utils';
import noteEditorStyles from "./noteEditorStyles";
import withStyles from "@material-ui/core/es/styles/withStyles";
import {Paper} from "@material-ui/core";
import {getInstance, useService} from "global-apps-common";
import NoteService from "../../modules/note/NoteService";
import CircularProgress from "@material-ui/core/CircularProgress";

function NoteEditorView({classes, content, ready, onChange}) {
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
        className={`${classes.noteEditor} scrollbar`}
        value={content}
        onChange={onChange}
        ref={input => {
          textInput = input
        }}
      />
    </Paper>
  );
}

function NoteEditor({classes, onInsert, onDelete, onReplace}) {
  const noteService = getInstance(NoteService);
  const {content, ready} = useService(noteService);

  const props = {
    classes,
    content,
    ready,

    onChange(event) {
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

  return <NoteEditorView {...props}/>
}

export default withStyles(noteEditorStyles)(NoteEditor);
