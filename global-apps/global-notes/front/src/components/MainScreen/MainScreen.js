import React from 'react';
import Header from "./Header/Header"
import SideBar from "./SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../../common/checkAuth';
import connectService from "../../common/connectService";
import NotesContext from "../../modules/notes/NotesContext";
import {withSnackbar} from "notistack";
import NotesService from "../../modules/notes/NotesService";
import AccountContext from "../../modules/account/AccountContext";
import EmptyNote from "./EmptyNote/EmptyNote";
import Note from "./Note/Note";

class MainScreen extends React.Component {
  constructor(props) {
    super(props);
    this.notesService = NotesService.create();
  }

  componentDidMount() {
    Promise.all([
      this.notesService.init(),
    ]).catch((err) => {
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
            <Note noteId={noteId}/>
          )}
        </div>
      </NotesContext.Provider>
    );
  }
}

export default connectService(
  AccountContext, (state, accountService) => ({accountService})
)(checkAuth(
  withSnackbar(withStyles(mainScreenStyles)(MainScreen))
  )
);
