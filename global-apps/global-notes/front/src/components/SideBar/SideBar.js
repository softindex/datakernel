import React from 'react';
import {withRouter} from 'react-router-dom';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import sideBarStyles from "./sideBarStyles";
import CreateNoteDialog from '../CreateNoteDialog/CreateNoteDialog';
import NotesList from '../NotesList/NotesList';
import connectService from '../../common/connectService';
import NotesContext from '../../modules/notes/NotesContext';
import DeleteNoteDialog from '../DeleteNoteDialog/DeleteNoteDialog';
import IconButton from "@material-ui/core/IconButton";
import Paper from "@material-ui/core/Paper";
import InputBase from "@material-ui/core/InputBase";
import SearchIcon from "@material-ui/icons/Search";
import Typography from "@material-ui/core/Typography";

class SideBar extends React.Component {
  state = {
    search: '',
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

  onSearchChange = event => {
    this.setState({
      search: event.target.value
    });
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

  getFilteredNotes() {
    if (this.state.search === '') {
      return Object.entries(this.props.notes)
    } else {
      return Object.entries(this.props.notes)
        .filter(([, name]) => name
          .toLowerCase()
          .includes(this.state.search.toLowerCase()))
    }
  }

  render() {
    const {classes, ready} = this.props;

    return (
      <div className={classes.wrapper}>
        <Paper className={classes.search}>
          <IconButton
            className={classes.iconButton}
            disabled={true}
          >
            <SearchIcon/>
          </IconButton>
          <InputBase
            className={classes.inputDiv}
            placeholder="Notes..."
            autoFocus
            value={this.state.search}
            onChange={this.onSearchChange}
            classes={{input: classes.input}}
          />
        </Paper>
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
            notes={this.getFilteredNotes()}
            notesService={this.props.notesService}
            ready={ready}
            onRename={this.showRenameDialog}
            onDelete={this.showDeleteDialog}
          />
          {this.getFilteredNotes().length === 0 && this.state.search !== '' && (
            <Typography
              className={classes.secondaryText}
              color="textSecondary"
              variant="body1"
            >
              Nothing found
            </Typography>
          )}
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
