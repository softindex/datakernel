import * as React from "react";
import {withStyles} from '@material-ui/core';
import clsx from 'clsx';
import profileStyles from './profileStyles'
import connectService from "../../common/connectService";
import AccountContext from "../../modules/account/AccountContext";
import Dialog from "../UIElements/Dialog/Dialog";
import DialogTitle from "@material-ui/core/DialogTitle";
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import TextField from "@material-ui/core/TextField";
import DialogActions from "@material-ui/core/DialogActions";
import Button from "@material-ui/core/Button";
import SaveIcon from '@material-ui/icons/Save';
import Tooltip from "@material-ui/core/Tooltip";
import IconButton from "@material-ui/core/IconButton";

class Profile extends React.Component {
  textField = React.createRef();

  copyToClipboard = () => {
    navigator.clipboard.writeText(this.textField.current.props.value);
  };

  render() {
    const {classes} = this.props;
    return (
      <Dialog
        open={this.props.open}
        className={classes.dialog}
        onClose={this.props.onClose}
        aria-labelledby="form-dialog-title"
      >
        <DialogTitle
          id="customized-dialog-title"
          onClose={this.props.onClose}
        >
          My Profile
        </DialogTitle>
        <DialogContent>
          <TextField
            className={classes.textField}
            value={this.props.publicKey}
            label="Public Key"
            autoFocus
            disabled={true}
            margin="normal"
            fullWidth
            ref={this.textField}
            type="text"
            variant="outlined"
            InputProps={{
              classes: { input: classes.input },
              endAdornment: (
                <IconButton
                  className={classes.iconButton}
                  onClick={this.copyToClipboard}
                >
                  <Tooltip title="Copy" aria-label="Copy">
                    <i className="material-icons">
                      file_copy
                    </i>
                  </Tooltip>
                </IconButton>
              ),
            }}
          >
          </TextField>
        </DialogContent>
        <DialogActions>
          <Button
            className={this.props.classes.logoutButton}
            color="secondary"
            onClick={this.props.logout}
          >
            Log Out
          </Button>
        </DialogActions>
      </Dialog>
    )
  }
}

export default connectService(
  AccountContext,
  ({publicKey}, contactsService) => ({publicKey,
    logout() {
      contactsService.logout();
    }
  })
)(
  withStyles(profileStyles)(Profile)
);
