import React from 'react';
import IconButton from '@material-ui/core/IconButton';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import contactMenuStyles from "./contactMenuStyles";
import {withStyles} from '@material-ui/core';
import {withSnackbar} from "notistack";

function ContactMenu({onAddContact, classes, enqueueSnackbar}) {
  const [anchorEl, setAnchorEl] = React.useState(null);
  const open = Boolean(anchorEl);

  function onClick(event) {
    if (open) {
      setAnchorEl(null);
    } else {
      setAnchorEl(event.currentTarget);
    }
  }

  function onClose() {
    setAnchorEl(null);
  }

  function onAdd() {
    onAddContact();
    onClose();
  }

  return (
    <div onClick={onClick} className={classes.wrapperButton}>
      <IconButton>
        <MoreVertIcon/>
      </IconButton>
      <Menu
        anchorEl={anchorEl}
        keepMounted
        open={open}
        onClose={onClose}
        classes={{paper: classes.menuPaper}}
      >
        <MenuItem
          key="Add Contact"
          onClick={onAdd}
        >
          Add Contact
        </MenuItem>
      </Menu>
    </div>
  );
}

export default withSnackbar(withStyles(contactMenuStyles)(ContactMenu));