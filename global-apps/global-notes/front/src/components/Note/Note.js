import React, {useMemo} from 'react';
import NoteService from '../../modules/note/NoteService';
import NoteEditor from '../NoteEditor/NoteEditor';
import {withSnackbar} from "notistack";
import {RegisterDependency, initService} from 'global-apps-common';

function Note({noteId, enqueueSnackbar}) {
  const noteService = useMemo(() => (
    NoteService.create(noteId)
  ), [noteId]);

  const onInsert = (position, content) => {
    noteService.insert(position, content);
  };

  const onDelete = (position, content) => {
    noteService.delete(position, content);
  };

  const onReplace = (position, oldContent, newContent) => {
    noteService.replace(position, oldContent, newContent);
  };

  initService(noteService, err => enqueueSnackbar(err.message, {
    variant: 'error'
  }));

  return (
    <RegisterDependency name={NoteService} value={noteService}>
      <NoteEditor
        onInsert={onInsert}
        onDelete={onDelete}
        onReplace={onReplace}
      />
    </RegisterDependency>
  );
}

export default withSnackbar(Note);
