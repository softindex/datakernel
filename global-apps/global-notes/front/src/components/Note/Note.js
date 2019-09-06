import React from 'react';
import NoteService from '../../modules/note/NoteService';
import NoteEditor from '../NoteEditor/NoteEditor';
import {withSnackbar} from "notistack";
import {RegisterDependency} from 'global-apps-common';

class Note extends React.Component {
  state = {
    noteId: null,
    noteService: null
  };

  onInsert = (position, content) => {
    this.state.noteService.insert(position, content);
  };

  onDelete = (position, content) => {
    this.state.noteService.delete(position, content);
  };

  onReplace = (position, oldContent, newContent) => {
    this.state.noteService.replace(position, oldContent, newContent);
  };

  static getDerivedStateFromProps(props, state) {
    if (props.noteId !== state.noteId) {
      if (state.noteService) {
        state.noteService.stop();
      }

      const noteService = NoteService.create(props.noteId);
      noteService.init()
        .catch(err => {
          props.enqueueSnackbar(err.message, {
            variant: 'error'
          });
        });

      return {
        noteId: props.noteId,
        noteService
      };
    }
    return state;
  }

  componentWillUnmount() {
    this.state.noteService.stop();
  }

  render() {
    return (
      <RegisterDependency name={NoteService} value={this.state.noteService}>
        <NoteEditor
          onInsert={this.onInsert}
          onDelete={this.onDelete}
          onReplace={this.onReplace}
        />
      </RegisterDependency>
    );
  }
}

export default withSnackbar(Note);
