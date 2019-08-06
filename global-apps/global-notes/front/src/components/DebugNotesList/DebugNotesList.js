import React from 'react';
import path from 'path';
import {withStyles} from '@material-ui/core';
import List from '@material-ui/core/List';
import CircularProgress from '@material-ui/core/CircularProgress';
import Grow from '@material-ui/core/Grow';
import NoteItem from '../DebugNoteItem/DebugNoteItem';
import notesListStyles from './notesListStyles';
import connectService from '../../common/connectService';
import NotesContext from '../../modules/notes/NotesContext';

function DebugNotesList({classes, ready, notes}) {
  const getNotePath = noteId => {
    return path.join('/debug', noteId || '');
  };

  return (
    <>
      {!ready && (
        <Grow in={!ready}>
          <div className={classes.progressWrapper}>
            <CircularProgress/>
          </div>
        </Grow>
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

export default connectService(
  NotesContext,
  ({notes, ready}, notesService) => ({notes, ready, notesService})
)(
  withStyles(notesListStyles)(DebugNotesList)
);
