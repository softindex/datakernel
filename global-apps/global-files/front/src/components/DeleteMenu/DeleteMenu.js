import React from 'react';
import Popover from '@material-ui/core/Popover';
import MenuList from '@material-ui/core/MenuList';
import MenuItem from '@material-ui/core/MenuItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import DeleteIcon from '@material-ui/icons/DeleteForeverOutlined';
import {withStyles} from "@material-ui/core";
import deleteMenuStyles from "./deleteMenuStyles";

function DeleteMenu({style, classes, deleteHandler}) {
  return (
    <Popover
      open={style.open}
      anchorReference="anchorPosition"
      anchorPosition={{
        top: style.top,
        left: style.left
      }}
    >
      <MenuList>
        <MenuItem button onClick={deleteHandler}>
          <ListItemIcon className={classes.listItemIcon}>
            <DeleteIcon/>
          </ListItemIcon>
          <ListItemText
            className={classes.listItemText}
            primary="Delete"
          />
        </MenuItem>
      </MenuList>
    </Popover>
  )
}

export default withStyles(deleteMenuStyles)(DeleteMenu);
