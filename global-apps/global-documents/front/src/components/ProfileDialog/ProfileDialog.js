import React, {useState} from "react";
import {Icon, withStyles} from '@material-ui/core';
import profileDialogStyles from './profileDialogStyles'
import {getInstance, useService} from "global-apps-common";
import Dialog from "../Dialog/Dialog";
import DialogTitle from "@material-ui/core/DialogTitle";
import DialogContent from "@material-ui/core/DialogContent";
import TextField from "@material-ui/core/TextField";
import DialogActions from "@material-ui/core/DialogActions";
import Button from "@material-ui/core/Button";
import Tooltip from "@material-ui/core/Tooltip";
import IconButton from "@material-ui/core/IconButton";
import {withSnackbar} from "notistack";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";
import MyProfileService from "../../modules/profile/MyProfileService";

function ProfileDialogView({
                             classes,
                             onClose,
                             loading,
                             profileReady,
                             profile,
                             name,
                             onSubmit,
                             onChangeName,
                             publicKey,
                             onDoubleClick,
                             copyToClipboard
                           }) {
  return (
    <Dialog
      onClose={onClose}
      loading={loading}
    >
      <DialogTitle>
        My Profile
      </DialogTitle>
      {!profileReady && (
        <div className={classes.progressWrapper}>
          <CircularProgress/>
        </div>
      )}
      {profileReady && (
        <form onSubmit={onSubmit}>
          <DialogContent classes={{root: classes.dialogContent}}>
            <TextField
              value={name}
              disabled={loading}
              margin="normal"
              label="Name"
              type="text"
              fullWidth
              onChange={onChangeName}
              variant="outlined"
            />
            <TextField
              className={classes.textField}
              value={publicKey}
              label="Public Key"
              autoFocus
              margin="normal"
              fullWidth
              inputProps={{onDoubleClick: onDoubleClick, id: 'inputId'}}
              type="text"
              variant="outlined"
              InputProps={{
                readOnly: true,
                classes: {input: classes.input},
                endAdornment: (
                  <IconButton
                    className={classes.iconButton}
                    onClick={copyToClipboard}
                    disabled={loading}
                  >
                    <Tooltip title="Copy">
                      <Icon>file_copy</Icon>
                    </Tooltip>
                  </IconButton>
                ),
              }}
            >
            </TextField>
          </DialogContent>
          <DialogActions>
            <Button
              className={classes.saveButton}
              color="primary"
              variant="contained"
              type="submit"
              disabled={loading}
            >
              Save
            </Button>
          </DialogActions>
        </form>
      )}
    </Dialog>
  );
}

function ProfileDialog({classes, enqueueSnackbar, publicKey, onClose}) {
  const profileService = getInstance(MyProfileService);
  const {profile, profileReady} = useService(profileService);
  const [loading, setLoading] = useState(false);
  const [name, setName] = useState(profile.name);

  const copyToClipboard = () => {
    navigator.clipboard.writeText(document.getElementById('inputId').value);
  };

  const onDoubleClick = event => {
    event.preventDefault();
    const input = document.getElementById('inputId');
    input.setSelectionRange(0, input.value.length);
  };

  const onChangeName = event => {
    setName(event.target.value)
  };

  const props = {
    classes,
    profile,
    publicKey,
    profileReady,
    name,
    loading,
    onClose,
    copyToClipboard,
    onDoubleClick,
    onChangeName,

    onSubmit(event) {
      event.preventDefault();

      if (profile.name === name) {
        return;
      }

      setLoading(true);
      profileService.setProfileField('name', name)
        .catch(error => enqueueSnackbar(error.message, {
          variant: 'error'
        }))
        .finally(() => {
          setLoading(false);
        });
    }
  };

  return <ProfileDialogView {...props}/>;
}

export default withSnackbar(withStyles(profileDialogStyles)(ProfileDialog));