import React from 'react';
import path from 'path';
import {withStyles} from '@material-ui/core';
import List from '@material-ui/core/List';
import CircularProgress from '@material-ui/core/CircularProgress';
import NoteItem from '../DebugNoteItem/DebugNoteItem';
import notesListStyles from './notesListStyles';
import {getInstance, useService} from "global-apps-common";
import NotesService from "../../modules/notes/NotesService";

function DebugNotesListView({classes, ready, notes, getNotePath}) {
  return (
    <>
      {!ready && (
        <div className={classes.progressWrapper}>
          <CircularProgress/>
        </div>
      )}
      {ready && (
        <div className={classes.notesList}>
          <List>
            {Object.entries(notes).map(([noteId, noteName], index) =>
              (
                <NoteItem
                  key={index}
                  noteId={noteId}
                  noteName={noteName}
                  getNotePath={getNotePath}
                />
              )
            )}
          </List>
        </div>
      )}
    </>
  );
}

function DebugNotesList({classes}) {
  const notesService = getInstance(NotesService);
  const {notes, ready} = useService(notesService);
  const props = {
    classes,
    notes,
    ready,

    getNotePath(noteId) {
      return path.join('/debug', noteId || '');
    }
  };

  return <DebugNotesListView {...props}/>
}

export default withStyles(notesListStyles)(DebugNotesList);
