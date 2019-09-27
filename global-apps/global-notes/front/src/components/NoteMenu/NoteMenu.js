import React, {useState} from 'react';
import {withStyles} from '@material-ui/core';
import IconButton from '@material-ui/core/IconButton';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import noteMenuStyles from './noteMenuStyles';

function NoteMenu({onRename, onDelete, classes}) {
  const [anchorEl, setAnchorEl] = useState(null);
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

  return (
    <div onClick={onClick} className={classes.wrapperButton}>
      <IconButton className={classes.iconButton}>
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
          key='Rename'
          onClick={() => {onRename(); onClose()}}
        >
          Rename
        </MenuItem>
        <MenuItem
          key='Delete'
          onClick={() => {onDelete(); onClose()}}
        >
          Delete
        </MenuItem>
      </Menu>
    </div>
  );
}

export default withStyles(noteMenuStyles)(NoteMenu);