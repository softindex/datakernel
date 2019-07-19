import React from 'react';
import {withStyles} from '@material-ui/core';
import PropTypes from 'prop-types';
import noteStyles from './noteStyles';
import NoteService from "../../../modules/note/NoteService";
import NoteContext from '../../../modules/note/NoteContext';
import NoteEditor from "../NoteEditor/NoteEditor";

class Note extends React.Component {
  static propTypes = {
    noteId: PropTypes.string.isRequired
  };

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

      const noteService = NoteService.from(props.noteId);
      noteService.init();

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

  update = newState => this.setState(newState);

  render() {
    const {classes} = this.props;
    return (
      <NoteContext.Provider value={this.state.noteService}>
        <div className={classes.root}>
          <div className={classes.headerPadding}/>
          <NoteEditor
            className={classes.noteEditor}
            onInsert={this.onInsert}
            onDelete={this.onDelete}
            onReplace={this.onReplace}/>
        </div>
      </NoteContext.Provider>
    );
  }
}

export default withStyles(noteStyles)(Note);
