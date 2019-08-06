import React, {useEffect} from 'react';
import {getDifference} from './utils';
import connectService from '../../common/connectService';
import NoteContext from '../../modules/note/NoteContext';
import nodeEditorStyles from "./nodeEditorStyles";
import withStyles from "@material-ui/core/es/styles/withStyles";

function NoteEditor(props) {
  let textInput = React.createRef();

  useEffect(() => {
    textInput.blur();
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
    <textarea
      className={props.classes.noteEditor}
      value={props.content}
      onChange={onChange}
      ref={input => {textInput = input}}
    />
  );
}

export default connectService(
  NoteContext,
  ({content}, noteService) => ({content, noteService})
)(
  withStyles(nodeEditorStyles)(NoteEditor)
);
