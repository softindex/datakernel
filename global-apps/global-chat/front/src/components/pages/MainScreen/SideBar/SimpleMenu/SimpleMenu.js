import React from 'react';
import IconButton from '@material-ui/core/IconButton';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import simpleMenuStyles from "./simpleMenuStyles";
import {withStyles} from '@material-ui/core';

const options = [
  'Add Contact'
];

function SimpleMenu({onAddContact, onDelete, classes}) {
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

  function handleDelete() {
    onDelete();
    handleClose();
  }

  return (
    <div onClick={handleClick} className={classes.wrapperButton} >
      <IconButton
        aria-label="More"
        aria-controls="simple-menu"
        aria-haspopup="true"
        className={classes.iconButton}
      >
        <MoreVertIcon />
      </IconButton>
        <Menu
          id="simple-menu"
          anchorEl={anchorEl}
          keepMounted
          open={open}
          onClose={handleClose}
          PaperProps={{
            style: {
              maxHeight: 48 * 4.5,
              width: 200,
            },
          }}
        >
          {options.map(option => (
            <MenuItem
              key={option}
              onClick={option === 'Add Contact' ? handleAdd : handleClose}
            >
              {option}
            </MenuItem>
          ))}
        </Menu>
    </div>
  );
}

export default withStyles(simpleMenuStyles)(SimpleMenu);