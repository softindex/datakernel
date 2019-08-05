import React from 'react';
import IconButton from '@material-ui/core/IconButton';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import documentMenuStyles from "./documentMenuStyles";
import {withStyles} from '@material-ui/core';

function DocumentMenu({onDeleteDocument, onRenameDocument, classes}) {
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
        <MenuItem onClick={() => {onRenameDocument(); onClose()}}
        >
          Rename document
        </MenuItem>
        <MenuItem onClick={() => {onDeleteDocument(); onClose()}}>
          Delete document
        </MenuItem>
      </Menu>
    </div>
  );
}

export default withStyles(documentMenuStyles)(DocumentMenu);
