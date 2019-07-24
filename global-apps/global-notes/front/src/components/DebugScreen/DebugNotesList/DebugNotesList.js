import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import NoteItem from "./DebugNoteItem/DebugNoteItem";
import notesListStyles from "./notesListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";
import connectService from "../../../common/connectService";
import NotesContext from "../../../modules/notes/NotesContext";

class DebugNotesList extends React.Component {

  getNotePath = (noteId) => {
    return path.join('/debug', noteId || '');
  };

  render() {
    const {classes, ready, notes} = this.props;
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
                    getNotePath={this.getNotePath}
                  />
                )
              )}
            </List>
          </div>
        )}
      </>
    );
  }
}

export default connectService(NotesContext, ({notes, ready}, notesService) => ({notes, ready, notesService}))(
  withStyles(notesListStyles)(DebugNotesList)
);
