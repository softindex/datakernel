import React, {useState} from 'react';
import {withRouter} from 'react-router-dom';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import sideBarStyles from "./sideBarStyles";
import CreateNoteDialog from '../CreateNoteDialog/CreateNoteDialog';
import NotesList from '../NotesList/NotesList';
import DeleteNoteDialog from '../DeleteNoteDialog/DeleteNoteDialog';
import IconButton from "@material-ui/core/IconButton";
import Paper from "@material-ui/core/Paper";
import InputBase from "@material-ui/core/InputBase";
import SearchIcon from "@material-ui/icons/Search";
import Typography from "@material-ui/core/Typography";
import {getInstance, useService} from "global-apps-common";
import NotesService from "../../modules/notes/NotesService";

function SideBarView({
                       classes,
                       ready,
                       search,
                       onSearchChange,
                       getFilteredNotes,
                       onShowCreateDialog,
                       showCreateDialog,
                       showRenameDialog,
                       showDeleteDialog,
                       closeDialogs,
                       renameDialog,
                       deleteDialog
                     }) {
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
          value={search}
          onChange={onSearchChange}
          classes={{input: classes.input}}
        />
      </Paper>
      <Button
        className={classes.button}
        fullWidth={true}
        variant="contained"
        size="medium"
        color="primary"
        onClick={onShowCreateDialog}
      >
        New Note
      </Button>
      <div className={`${classes.notesList} scroller`}>
        <NotesList
          notes={getFilteredNotes()}
          ready={ready}
          onRename={showRenameDialog}
          onDelete={showDeleteDialog}
        />
        {getFilteredNotes().length === 0 && search !== '' && (
          <Typography
            className={classes.secondaryText}
            color="textSecondary"
            variant="body1"
          >
            Nothing found
          </Typography>
        )}
      </div>
      {(showCreateDialog || renameDialog.show) && (
        <CreateNoteDialog
          onClose={closeDialogs}
          rename={renameDialog}
        />
      )}
      {deleteDialog.show && (
        <DeleteNoteDialog
          currentNoteId={deleteDialog.noteId}
          onClose={closeDialogs}
        />
      )}
    </div>
  );
}

function SideBar({classes}) {
  const notesService = getInstance(NotesService);
  const {notes, ready} = useService(notesService);
  const [search, setSearch] = useState('');
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [deleteDialog, setDeleteDialog] = useState({show: false, noteId: null});
  const [renameDialog, setRenameDialog] = useState({show: false, noteId: null, noteName: ''});

  const props = {
    classes,
    ready,
    search,
    deleteDialog,
    renameDialog,
    showCreateDialog,

    onSearchChange(event) {
      setSearch(event.target.value);
    },

    onShowCreateDialog() {
      setShowCreateDialog(true);
    },

    showDeleteDialog(noteId) {
      setDeleteDialog({
        show: true,
        noteId
      });
    },

    showRenameDialog(noteId, noteName) {
      setRenameDialog({
        show: true,
        noteId,
        noteName
      });
    },

    closeDialogs() {
      setShowCreateDialog(false);
      setRenameDialog({
        show: false,
        noteId: null,
        noteName: ''
      });
      setDeleteDialog({
        show: false,
        noteId: null
      });
    },

    getFilteredNotes() {
      if (search === '') {
        return Object.entries(notes)
      } else {
        return Object.entries(notes)
          .filter(([, name]) => name
            .toLowerCase()
            .includes(search.toLowerCase()))
      }
    }
  };

  return <SideBarView {...props}/>
}

export default withRouter(withStyles(sideBarStyles)(SideBar));
