import React, {useMemo} from 'react';
import {withSnackbar} from 'notistack';
import {withStyles} from '@material-ui/core';
import Header from '../Header/Header';
import SideBar from '../SideBar/SideBar';
import mainScreenStyles from './mainScreenStyles';
import NotesService from '../../modules/notes/NotesService';
import Note from '../Note/Note';
import EmptyNote from '../EmptyNote/EmptyNote'
import {
  checkAuth,
  initService,
  AuthContext,
  connectService,
  RegisterDependency,
  useService
} from 'global-apps-common';

function MainScreen({match, enqueueSnackbar, classes, publicKey}) {
  const {notesService} = useMemo(() => {
    const notesService = NotesService.create();
    return {
      notesService
    }
  }, [publicKey]);

  function errorHandler(err) {
    enqueueSnackbar(err.message, {
      variant: 'error'
    });
  }

  initService(notesService, errorHandler);
  const {noteId} = match.params;
  const {notes} = useService(notesService);

  return (
    <RegisterDependency name={NotesService} value={notesService}>
      <Header title={notes[noteId] || ''}/>
      <div className={classes.note}>
        <SideBar/>
        {!noteId && (
          <EmptyNote title='Please select note to start editing'/>
        )}
        {noteId && (
          <Note noteId={noteId}/>
        )}
      </div>
    </RegisterDependency>
  )
}

export default connectService(
  AuthContext, ({publicKey}, accountService) => ({
    publicKey, accountService
  })
)(
  checkAuth(
    withSnackbar(
      withStyles(mainScreenStyles)(MainScreen)
    )
  )
);

