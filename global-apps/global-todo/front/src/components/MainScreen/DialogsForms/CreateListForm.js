import React from "react";
import {withStyles} from '@material-ui/core';
import formStyles from "./formStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../../common/Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import {withSnackbar} from "notistack";
import * as PropTypes from "prop-types";

class CreateListForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      name: '',
      loading: false,
    };
  }

  handleNameChange = (event) => {
    this.setState({
      name: event.target.value
    });
  };

  handleSubmit = (event) => {
    event.preventDefault();

    const {onCreate, onClose, history} = this.props;

    this.setState({
      loading: true
    });

    const newListId = onCreate(this.state.name);
    onClose();
    history.push('/' + newListId);
    this.setState({
      name: '',
      loading: false
    });
  };

  render() {
    const {open, onClose, classes} = this.props;
    return (
      <Dialog
        open={open}
        onClose={onClose}
        aria-labelledby="form-dialog-title"
      >
        <form onSubmit={this.handleSubmit}>
          <DialogTitle
            id="customized-dialog-title"
            onClose={onClose}
          >
            Create todo list
          </DialogTitle>
          <DialogContent>
            <TextField
              required={true}
              autoFocus
              value={this.state.name}
              disabled={this.state.loading}
              margin="normal"
              label="List name"
              type="text"
              fullWidth
              variant="outlined"
              onChange={this.handleNameChange}
            />
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              onClick={() => {
                onClose();
                this.setState({
                  ...this.state,
                  name: ''
                })
              }}
            >
              Close
            </Button>
            <Button
              className={classes.actionButton}
              type={"submit"}
              color={"primary"}
              variant={"contained"}
            >
              Create
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    );
  }
}

CreateListForm.propTypes = {
  enqueueSnackbar: PropTypes.func.isRequired,
};

export default withSnackbar(withStyles(formStyles)(CreateListForm));
