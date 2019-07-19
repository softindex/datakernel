import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import NoteItem from "./NoteItem/NoteItem";
import notesListStyles from "./notesListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

class NotesList extends React.Component {
  onNoteCreate(noteName) {
    return this.props.createNote(noteName);
  }

  getNotePath = (noteId) => {
    return path.join('/note', noteId || '');
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
                    showMenuIcon={true}
                    onRename={this.props.onRename}
                    onDelete={this.props.onDelete}
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

export default withStyles(notesListStyles)(NotesList);
