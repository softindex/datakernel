import React from 'react';
import {withRouter} from 'react-router-dom';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import sideBarStyles from "./sideBarStyles";
import CreateNoteDialog from '../NoteDialogs/CreateNoteDialog';
import NotesList from './NotesList/NotesList';
import connectService from '../../../common/connectService';
import NotesContext from '../../../modules/notes/NotesContext';
import DeleteNoteDialog from '../NoteDialogs/DeleteNoteDialog';

class SideBar extends React.Component {
  state = {
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

    return (
      <div className={classes.wrapper}>
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
            ready={ready}
            onRename={this.showRenameDialog}
            onDelete={this.showDeleteDialog}
          />
        </div>
        <CreateNoteDialog
          open={this.state.showCreateDialog || this.state.renameDialog.show}
          onClose={this.closeDialogs}
          rename={this.state.renameDialog}
        />
        <DeleteNoteDialog
          open={this.state.deleteDialog.show}
          noteId={this.state.deleteDialog.noteId}
          onClose={this.closeDialogs}
        />
      </div>
    );
  }
}

export default withRouter(
  connectService(
    NotesContext,
    ({ready, notes}, notesService) => ({
      notesService, ready, notes,
    })
  )(
    withStyles(sideBarStyles)(SideBar)
  )
);
