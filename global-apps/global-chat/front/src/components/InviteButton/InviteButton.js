import React from 'react';
import {Icon, TextField, withStyles} from '@material-ui/core';
import AddIcon from '@material-ui/icons/Add';
import Button from '@material-ui/core/Button';
import inviteButtonStyles from "./inviteButtonStyles";
import IconButton from "@material-ui/core/IconButton";
import Tooltip from "@material-ui/core/Tooltip";

function InviteButton({classes, publicKey}) {
  const [showTextField, setShowTextField] = React.useState(false);

  function onClick() {
    setShowTextField(true);
  }

  const copyToClipboard = () => {
    navigator.clipboard.writeText(document.getElementById('inviteInputId').value);
  };

  const onDoubleClick = event => {
    event.preventDefault();
    const input = document.getElementById('inviteInputId');
    input.setSelectionRange(0, input.value.length);
  };

  return (
    <>
      {!showTextField && (
        <Button
          variant="outlined"
          size="medium"
          fullWidth={true}
          color="primary"
          className={classes.inviteButton}
          onClick={onClick}
        >
          <AddIcon className={classes.addIcon}/>
          Invite Friends
        </Button>
      )}
      {showTextField && (
        <TextField
          className={classes.textField}
          value={window.location.host + "/invite/" + publicKey}
          label="Invite Link"
          autoFocus
          margin="normal"
          inputProps={{onDoubleClick: onDoubleClick, id: 'inviteInputId'}}
          type="text"
          variant="outlined"
          InputProps={{
            readOnly: true,
            endAdornment: (
              <IconButton
                className={classes.iconButton}
                onClick={copyToClipboard}
              >
                <Tooltip title="Copy">
                  <Icon>file_copy</Icon>
                </Tooltip>
              </IconButton>
            ),
          }}
        >
        </TextField>
      )}
    </>
  );
}

export default withStyles(inviteButtonStyles)(InviteButton);