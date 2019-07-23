import React from 'react';
import Header from "./Header/Header"
import SideBar from "./SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../../common/checkAuth';
import NotesContext from "../../modules/notes/NotesContext";
import {withSnackbar} from "notistack";
import NotesService from "../../modules/notes/NotesService";
import EmptyNote from "./EmptyNote/EmptyNote";
import Note from "./Note/Note";

class MainScreen extends React.Component {
  constructor(props) {
    super(props);
    this.notesService = NotesService.create();
  }

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
        <Header noteId={noteId}/>
        <div className={this.props.classes.note}>
          <SideBar
            history={this.props.history}
            match={this.props.match}
          />
          {!noteId && (
            <EmptyNote/>
          )}
          {noteId && (
            <Note
              noteId={noteId}
              isNew={this.notesService.state.newNotes.has(noteId)}/>
          )}
        </div>
      </NotesContext.Provider>
    );
  }
}

export default checkAuth(
  withSnackbar(
    withStyles(mainScreenStyles)(MainScreen)
  )
);
