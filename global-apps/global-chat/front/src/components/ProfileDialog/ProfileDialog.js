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
import MyProfileService from "../../modules/myProfile/MyProfileService";

function ProfileDialogView(props) {
  return (
    <Dialog
      open={props.open}
      onClose={props.onClose}
      loading={props.loading}
    >
      <DialogTitle onClose={props.onClose}>
        My Profile
      </DialogTitle>
      {!props.profileReady && (
        <Grow in={!props.profileReady}>
          <div className={props.classes.progressWrapper}>
            <CircularProgress/>
          </div>
        </Grow>
      )}
      {props.profileReady && (
        <form onSubmit={props.onSubmit}>
          <DialogContent classes={{root: props.classes.dialogContent}}>
            <TextField
              className={props.classes.textField}
              defaultValue={props.name === null ? props.profile.name : props.name}
              disabled={props.loading}
              margin="normal"
              label="Name"
              type="text"
              fullWidth
              onChange={props.onChangeName}
              variant="outlined"
            />
            <TextField
              className={props.classes.textField}
              value={props.publicKey}
              label="Public Key"
              autoFocus
              margin="normal"
              fullWidth
              inputProps={{onDoubleClick: props.onDoubleClick, id: 'inputId'}}
              type="text"
              variant="outlined"
              InputProps={{
                readOnly: true,
                classes: {input: props.classes.input},
                endAdornment: (
                  <IconButton
                    className={props.classes.iconButton}
                    onClick={props.copyToClipboard}
                    disabled={props.loading}
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
              className={props.classes.saveButton}
              color="primary"
              variant="contained"
              type="submit"
              disabled={props.loading}
            >
              Save
            </Button>
          </DialogActions>
        </form>
      )}
    </Dialog>
  );
}

function ProfileDialog(props) {
  const profileService = getInstance(MyProfileService);
  const {profile, profileReady} = useService(profileService);
  const [loading, setLoading] = useState(false);
  const [name, setName] = useState(null);

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

  const onSubmit = event => {
    event.preventDefault();
    setLoading(true);

    return props.onSetProfileField('name', name)
      .catch(err => {
        props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      })
      .finally(() => {
        setLoading(false);
      });
  };

  props = {
    ...props,
    profile,
    profileReady,
    name,
    loading,
    copyToClipboard,
    onSubmit,
    onDoubleClick,
    onChangeName,
    onSetProfileField(fieldName, value) {
      return profileService.setProfileField(fieldName, value);
    }
  };

  return <ProfileDialogView {...props}/>;
}

export default withSnackbar(withStyles(profileDialogStyles)(ProfileDialog));
