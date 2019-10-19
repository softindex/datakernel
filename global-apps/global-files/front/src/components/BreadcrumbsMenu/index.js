import React from "react";
import Menu from "@material-ui/core/Menu";
import MenuList from "@material-ui/core/MenuList";
import MenuItem from "@material-ui/core/MenuItem";
import ListItemIcon from "@material-ui/core/ListItemIcon";
import DeleteIcon from '@material-ui/icons/DeleteForeverOutlined';
import ListItemText from "@material-ui/core/ListItemText";
import {withStyles} from "@material-ui/core";
import breadcrumbsMenuStyles from "./breadcrumbsMenuStyles";

function BreadcrumbsMenu({classes, onClose, onDelete, anchorElement}) {
    return (
      <Menu
        disableAutoFocusItem
        open={Boolean(anchorElement)}
        anchorEl={anchorElement}
        onClose={onClose}
      >
        <MenuList>
          <MenuItem button onClick={onDelete}>
            <ListItemIcon className={classes.listItemIcon}>
              <DeleteIcon/>
            </ListItemIcon>
            <ListItemText
              className={classes.listItemText}
              primary="Delete"
            />
          </MenuItem>
        </MenuList>
      </Menu>
    )
}

export default withStyles(breadcrumbsMenuStyles)(BreadcrumbsMenu);
