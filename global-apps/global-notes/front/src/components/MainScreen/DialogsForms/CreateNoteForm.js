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

class CreateNoteForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      name: '',
      loading: false
    };
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

    return this.props.createNote(this.state.name)
      .then(newNoteId => {
        this.props.onClose();
        this.props.history.push('/note/' + newNoteId);
      })
      .catch(err => {
        this.props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      })
      .finally(() => {
        this.setState({
          name: '',
          loading: false
        });
      });
  };

  handleClose = () => {
    this.props.onClose();
    this.setState({
      name: ''
    });
  }

  render() {
    return (
      <Dialog
        open={this.props.open}
        onClose={this.handleClose}
        aria-labelledby="form-dialog-title"
      >
        <form onSubmit={this.handleSubmit}>
          <DialogTitle
            id="customized-dialog-title"
            onClose={this.handleClose}
          >
            Create note
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
              onChange={this.handleNameChange}
            />
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              onClick={this.handleClose}
            >
              Close
            </Button>
            <Button
              className={this.props.classes.actionButton}
              type="submit"
              color="primary"
              variant="contained"
            >
              Create
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    );
  }
}

CreateNoteForm.propTypes = {
  enqueueSnackbar: PropTypes.func.isRequired,
};

export default connectService(
  NotesContext, (state, notesService) => ({
    createNote(name) {
      return notesService.createNote(name);
    }
  })
)(
  withSnackbar(withStyles(formStyles)(CreateNoteForm))
);
