import React from 'react';
import IconButton from '@material-ui/core/IconButton';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import contactMenuStyles from "./contactMenuStyles";
import {withStyles} from '@material-ui/core';

function ContactMenu({onAddContact, classes}) {
  const [anchorEl, setAnchorEl] = React.useState(null);
  const open = Boolean(anchorEl);

  function handleClick(event) {
    if (open) {
      setAnchorEl(null);
    } else {
      setAnchorEl(event.currentTarget);
    }
  }

  function handleClose() {
    setAnchorEl(null);
  }

  function handleAdd() {
    onAddContact();
    handleClose();
  }
  //can't delete div because of incorrect handling click menu}
  return (
    <div onClick={handleClick} className={classes.wrapperButton}>
      <IconButton>
        <MoreVertIcon/>
      </IconButton>
      <Menu
        anchorEl={anchorEl}
        keepMounted
        open={open}
        onClose={handleClose}
        classes={{paper: classes.menuPaper}}
      >
        <MenuItem
          key="Add Contact"
          onClick={handleAdd}
        >
          Add Contact
        </MenuItem>
      </Menu>
    </div>
  );
}

export default withStyles(contactMenuStyles)(ContactMenu);