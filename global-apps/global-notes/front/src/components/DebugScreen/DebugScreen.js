import DebugNotesList from "./DebugNotesList/DebugNotesList";
import NotesContext from "../../modules/notes/NotesContext"
import React from "react";
import NotesService from "../../modules/notes/NotesService";
import connectService from "../../common/connectService";
import AccountContext from "../../modules/account/AccountContext";
import checkAuth from "../../common/checkAuth";
import {withSnackbar} from "notistack";
import CommitsGraph from "./CommitsGraph/CommitsGraph";
import Grid from '@material-ui/core/Grid';

class DebugScreen extends React.Component {
  notesService = NotesService.create();

  componentDidMount() {
    this.notesService.init()
      .catch((err) => {
        this.props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      });
  }

  componentWillUnmount() {
    this.notesService.stop();
  }

  render() {
    const {noteId} = this.props.match.params;
    return (
      <NotesContext.Provider value={this.notesService}>
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
}

export default connectService(
  AccountContext, (state, accountService) => ({accountService})
)(checkAuth(
  withSnackbar(DebugScreen)
  )
);
