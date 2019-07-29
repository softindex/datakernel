import React, {useEffect} from 'react';
import {withSnackbar} from 'notistack';
import {withStyles} from '@material-ui/core';
import Header from './Header/Header';
import SideBar from './SideBar/SideBar';
import mainScreenStyles from './mainScreenStyles';
import checkAuth from '../../common/checkAuth';
import NotesContext from '../../modules/notes/NotesContext';
import NotesService from '../../modules/notes/NotesService';
import EmptyNote from './EmptyNote/EmptyNote';
import Note from './Note/Note';

function MainScreen(props) {
  const notesService = NotesService.create();
  const {noteId} = props.match.params;

  useEffect(() => {
    notesService.init()
      .catch(err => {
        props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      });

    return () => {
      notesService.stop();
    };
  });

  return (
    <NotesContext.Provider value={notesService}>
      <Header noteId={noteId}/>
      <div className={props.classes.note}>
        <SideBar
          history={props.history}
          match={props.match}
        />
        {!noteId && (
          <EmptyNote/>
        )}
        {noteId && (
          <Note
            noteId={noteId}
            isNew={notesService.state.newNotes.has(noteId)}/>
        )}
      </div>
    </NotesContext.Provider>
  )
}

export default checkAuth(
  withSnackbar(
    withStyles(mainScreenStyles)(MainScreen)
  )
);
