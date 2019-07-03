import React from 'react';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import profileMenuStyles from "./profileMenuStyles";
import {withStyles} from '@material-ui/core';
import ListItemIcon from "@material-ui/core/ListItemIcon";
import AccountCircle from "@material-ui/icons/AccountCircle";

const options = [
  'Profile',
  'Log Out'
];

function ProfileMenu({logout, onOpenProfile, classes}) {
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

  function handleLogOut() {
    logout();
    handleClose();
  }

  function handleOpenProfile() {
    onOpenProfile();
    handleClose();
  }

  return (
    <div onClick={handleClick} className={classes.wrapperButton} >
      <div
        aria-label="More"
        aria-controls="simple-menu"
        aria-haspopup="true"
        color="inherit"
        className={classes.iconButton}
      >
        <MoreVertIcon />
      </div>
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
            onClick={option === 'Profile' ? handleOpenProfile : handleLogOut}
          >
            {option === 'Profile' && (
              <ListItemIcon>
                <AccountCircle className={classes.accountIcon}/>
              </ListItemIcon>
            )}
            {option === 'Log Out' && (
              <ListItemIcon>
                <span
                  className="iconify"
                  data-icon="mdi-logout"
                  data-inline="false"
                  style={{fontSize: 30}}
                />
              </ListItemIcon>
            )}
            {option}
          </MenuItem>
        ))}
      </Menu>
    </div>
  );
}

export default withStyles(profileMenuStyles)(ProfileMenu);