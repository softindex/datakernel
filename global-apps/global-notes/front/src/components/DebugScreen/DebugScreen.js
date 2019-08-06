import React, {useEffect} from 'react';
import {withSnackbar} from 'notistack';
import Grid from '@material-ui/core/Grid';
import DebugNotesList from '../DebugNotesList/DebugNotesList';
import NotesContext from '../../modules/notes/NotesContext';
import NotesService from '../../modules/notes/NotesService';
import connectService from '../../common/connectService';
import AccountContext from '../../modules/account/AccountContext';
import checkAuth from '../../common/checkAuth';
import CommitsGraph from '../CommitsGraph/CommitsGraph';

function DebugScreen(props) {
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
      <Grid container>
        <Grid item xs={3}>
          <DebugNotesList/>
        </Grid>
        <Grid item xs={9}>
          {noteId && (<CommitsGraph noteId={noteId}/>)}
        </Grid>
      </Grid>
    </NotesContext.Provider>
  );
}

export default connectService(
  AccountContext,
  (state, accountService) => ({accountService})
)(
  checkAuth(
    withSnackbar(DebugScreen)
  )
);
