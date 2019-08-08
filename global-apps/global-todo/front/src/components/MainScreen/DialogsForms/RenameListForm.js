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

class RenameListForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      name: '',
      loading: false,
    };
  }

  componentDidUpdate(prevProps, prevState, snapshot) {
    if (prevProps.listName !== this.props.listName) {
      this.setState({
        ...this.state,
        name: this.props.listName
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

    this.props.onRename(this.state.name);
    this.props.onClose();
    this.setState({
      loading: false,
      name: ''
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
            Rename todo list
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
              type={"submit"}
              color={"primary"}
              variant={"contained"}
            >
              Rename
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    );
  }
}

RenameListForm.propTypes = {
  enqueueSnackbar: PropTypes.func.isRequired,
};

export default withSnackbar(withStyles(formStyles)(RenameListForm));
