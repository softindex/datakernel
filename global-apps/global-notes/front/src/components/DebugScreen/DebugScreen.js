import React, {useEffect} from 'react';
import {withSnackbar} from 'notistack';
import Grid from '@material-ui/core/Grid';
import DebugNotesList from '../DebugNotesList/DebugNotesList';
import NotesService from '../../modules/notes/NotesService';
import {checkAuth, RegisterDependency} from 'global-apps-common';
import CommitsGraph from '../CommitsGraph/CommitsGraph';

function DebugScreen({match, enqueueSnackbar}) {
  const notesService = NotesService.create();
  const {noteId} = match.params;

  useEffect(() => {
    notesService.init()
      .catch(err => {
        enqueueSnackbar(err.message, {
          variant: 'error'
        });
      });

    return () => {
      notesService.stop();
    };
  });

  return (
    <RegisterDependency name={NotesService} value={notesService}>
      <Grid container>
        <Grid item xs={3}>
          <DebugNotesList/>
        </Grid>
        <Grid item xs={9}>
          {noteId && (<CommitsGraph noteId={noteId}/>)}
        </Grid>
      </Grid>
    </RegisterDependency>
  );
}

export default checkAuth(withSnackbar(DebugScreen));
