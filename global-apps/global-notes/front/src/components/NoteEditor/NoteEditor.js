import React, {useEffect} from 'react';
import {getDifference} from './utils';
import connectService from '../../common/connectService';
import NoteContext from '../../modules/note/NoteContext';
import noteEditorStyles from "./noteEditorStyles";
import withStyles from "@material-ui/core/es/styles/withStyles";
import {Paper} from "@material-ui/core";

function NoteEditor(props) {
  let textInput = React.createRef();

  useEffect(() => {
    textInput.focus();
  }, [textInput]);

  const onChange = event => {
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
        className={props.classes.noteEditor}
        value={props.content}
        onChange={onChange}
        ref={input => {
          textInput = input
        }}
      />
    </Paper>
  );
}

export default connectService(
  NoteContext,
  ({content}, noteService) => ({content, noteService})
)(
  withStyles(noteEditorStyles)(NoteEditor)
);
