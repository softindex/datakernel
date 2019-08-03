import React from 'react';
import {withSnackbar} from 'notistack';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import noteDialogsStyles from './noteDialogsStyles';
import Dialog from '../../common/Dialog/Dialog'
import connectService from '../../../common/connectService';
import NotesContext from '../../../modules/notes/NotesContext';
import {withRouter} from "react-router-dom";

class CreateNoteDialog extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      name: props.rename.noteName || '',
      loading: false
    };
  }

  componentDidUpdate(prevProps, prevState) {
    if (prevProps.rename.noteName !== this.props.rename.noteName) {
      this.setState({
        name: this.props.rename.noteName
      });
    }
  }

  onNameChange = event => {
    this.setState({
      name: event.target.value
    });
  };

  onSubmit = event => {
    event.preventDefault();
    this.setState({
      loading: true
    });

    this.props.onSubmit(this.props.rename.noteId, this.state.name);

    this.setState({
      name: '',
      loading: false
    });
  };

  onClose = () => {
    this.props.onClose();
    this.setState({
      name: ''
    });
  };

  render() {
    return (
      <Dialog
        open={this.props.open}
        onClose={this.onClose}
      >
        <form onSubmit={this.onSubmit}>
          <DialogTitle onClose={this.onClose}>
            {this.props.rename.show ? 'Rename note' : 'Create note'}
          </DialogTitle>
          <DialogContent>
            <TextField
              required={true}
              autoFocus
              value={this.state.name}
              disabled={this.state.loading}
              margin="normal"
              label="Note name"
              type="text"
              fullWidth
              variant="outlined"
              onChange={this.onNameChange}
            />
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              onClick={this.onClose}
            >
              Close
            </Button>
            <Button
              className={this.props.classes.actionButton}
              type="submit"
              color="primary"
              variant="contained"
            >
              {this.props.rename.show ? 'Rename' : 'Create'}
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    );
  }
}

export default withRouter(
  connectService(
    NotesContext, (state, notesService, props) => ({
      onSubmit(id, name) {
        if (!props.rename.show) {
          notesService.createNote(name)
            .then(newNoteId => {
              props.onClose();
              props.history.push('/note/' + newNoteId);
            })
            .catch(err => {
              props.enqueueSnackbar(err.message, {
                variant: 'error'
              });
            });
        } else {
          notesService.renameNote(id, name)
            .then(() => {
              props.onClose();
            })
            .catch(err => {
              props.enqueueSnackbar(err.message, {
                variant: 'error'
              });
            });
        }
      }
    })
  )(
    withSnackbar(withStyles(noteDialogsStyles)(CreateNoteDialog))
  )
);
