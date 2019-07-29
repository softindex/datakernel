import React from 'react';
import {Redirect} from 'react-router-dom';
import {withStyles} from '@material-ui/core';
import Typography from '@material-ui/core/Typography';
import Paper from '@material-ui/core/Paper';
import Button from '@material-ui/core/Button';
import sideBarStyles from "./sideBarStyles";
import CreateNoteForm from '../DialogsForms/CreateNoteForm';
import NotesList from './NotesList/NotesList';
import connectService from '../../../common/connectService';
import NotesContext from '../../../modules/notes/NotesContext';
import DeleteNoteForm from '../DialogsForms/DeleteNoteForm';
import RenameNoteForm from '../DialogsForms/RenameNoteForm';

class SideBar extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      showCreateDialog: false,
      deleteDialog: {
        show: false,
        noteId: null
      },
      renameDialog: {
        show: false,
        noteId: null,
        noteName: ''
      }
    }
  };

  showCreateDialog = () => this.setState({showCreateDialog: true});

  showDeleteDialog = noteId => this.setState({
    deleteDialog: {
      show: true,
      noteId
    }
  });

  showRenameDialog = (noteId, noteName) => this.setState({
    renameDialog: {
      show: true,
      noteId,
      noteName
    }
  });

  closeDialogs = () => {
    this.setState({
      showCreateDialog: false,
      deleteDialog: {
        show: false,
        noteId: null
      },
      renameDialog: {
        show: false,
        noteId: null,
        noteName: ''
      }
    });
  };

  render() {
    const {classes, notes, ready} = this.props;
    const {noteId} = this.props.match.params;

    if (noteId && ready && !notes[noteId]){
      return <Redirect to='/'/>;
    }

    return (
      <div className={classes.wrapper}>
        <Paper square className={classes.paper}/>
        <Typography
          className={classes.tabContent}
          component="div"
        >
          <CreateNoteForm
            history={this.props.history}
            open={this.state.showCreateDialog}
            onClose={this.closeDialogs}
          />
          <DeleteNoteForm
            open={this.state.deleteDialog.show}
            noteId={this.state.deleteDialog.noteId}
            onClose={this.closeDialogs}
          />
          <RenameNoteForm
            open={this.state.renameDialog.show}
            noteId={this.state.renameDialog.noteId}
            noteName={this.state.renameDialog.noteName}
            onClose={this.closeDialogs}
          />
          <Button
            className={classes.button}
            fullWidth={true}
            variant="contained"
            size="medium"
            color="primary"
            onClick={this.showCreateDialog}
          >
            New Note
          </Button>
          <div className={classes.notesList}>
            <NotesList
              notes={notes}
              notesService={this.props.notesService}
              ready={this.props.ready}
              onRename={this.showRenameDialog}
              onDelete={this.showDeleteDialog}
            />
          </div>
        </Typography>
      </div>
    );
  }
}

export default connectService(
  NotesContext,
  ({ready, notes}, notesService) => ({
    notesService, ready, notes,
  })
)(
  withStyles(sideBarStyles)(SideBar)
);
