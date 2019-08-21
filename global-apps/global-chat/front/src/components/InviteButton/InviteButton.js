import React from 'react';
import {Paper, withStyles} from '@material-ui/core';
import AddIcon from '@material-ui/icons/Add';
import Popover from '@material-ui/core/Popover';
import Typography from '@material-ui/core/Typography';
import Button from '@material-ui/core/Button';
import inviteButtonStyles from "./inviteButtonStyles";

function InviteButton({classes}) {
  const [anchorEl, setAnchorEl] = React.useState(null);

  function onClick(event) {
    setAnchorEl(event.currentTarget);
  }

  function onClose() {
    setAnchorEl(null);
  }

  const open = Boolean(anchorEl);
  return (
    <div>
      <Button
        variant="outlined"
        size="medium"
        fullWidth={true}
        color="primary"
        className={classes.inviteButton}
        onClick={onClick}
      >
        Invite Friends
        <AddIcon className={classes.addIcon}/>
      </Button>
      <Popover
        open={open}
        anchorEl={anchorEl}
        onClose={onClose}
        classes={{paper: classes.invitePaper}}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'center',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
      >
        <Typography variant="h6">Invite friends with this link:</Typography>
        <Typography variant="subtitle1">url link</Typography>
        <Button size="small" variant="outlined" className={classes.button}>
          Copy Link
        </Button>
      </Popover>
    </div>
  );
}

export default withStyles(inviteButtonStyles)(InviteButton)