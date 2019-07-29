import React from 'react';
import * as PropTypes from 'prop-types';
import {withSnackbar} from 'notistack';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import formStyles from './formStyles';
import Dialog from '../../common/Dialog/Dialog'
import connectService from '../../../common/connectService';
import NotesContext from '../../../modules/notes/NotesContext';

class RenameNoteForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      name: '',
      loading: false,
    };
  }

  componentDidUpdate(prevProps, prevState, snapshot) {
    if (prevProps.noteName !== this.props.noteName) {
      this.setState({
        name: this.props.noteName
      });
    }
  }

  handleNameChange = (event) => {
    this.setState({
      name: event.target.value
    });
  };

  handleSubmit = (event) => {
    event.preventDefault();

    this.setState({
      loading: true
    });

    return this.props.renameNote(this.props.noteId, this.state.name)
      .then(() => {
        this.props.onClose();
      })
      .catch(err => {
        this.props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      })
      .finally(() => {
        this.setState({
          loading: false,
          name: ''
        });
      });
  };

  render() {
    return (
      <Dialog
        open={this.props.open}
        onClose={this.props.onClose}
        aria-labelledby="form-dialog-title"
      >
        <form onSubmit={this.handleSubmit}>
          <DialogTitle
            id="customized-dialog-title"
            onClose={this.props.onClose}
          >
            Rename note
          </DialogTitle>
          <DialogContent>
            <TextField
              required={true}
              autoFocus
              value={this.state.name}
              disabled={this.state.loading}
              margin="normal"
              label="New name"
              type="text"
              fullWidth
              variant="outlined"
              onChange={this.handleNameChange}
            />
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              onClick={this.props.onClose}
            >
              Close
            </Button>
            <Button
              className={this.props.classes.actionButton}
              type="submit"
              color="primary"
              variant="contained"
            >
              Rename
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    );
  }
}

RenameNoteForm.propTypes = {
  enqueueSnackbar: PropTypes.func.isRequired,
};

export default connectService(
  NotesContext,
  (state, notesService) => ({
    renameNote(noteId, noteName) {
      return notesService.renameNote(noteId, noteName);
    }
  })
)(
  withSnackbar(withStyles(formStyles)(RenameNoteForm))
);
