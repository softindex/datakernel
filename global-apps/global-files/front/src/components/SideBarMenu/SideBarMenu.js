import {Link} from "react-router-dom";
import ListItem from "@material-ui/core/ListItem";
import ListItemIcon from "@material-ui/core/ListItemIcon";
import ListItemText from "@material-ui/core/ListItemText";
import StorageIcon from '@material-ui/icons/StorageOutlined';
import List from "@material-ui/core/List";
import React from "react";
import {withStyles} from "@material-ui/core";
import sideBarMenuStyles from "./sideBarMenuStyles";

function SideBarMenu({classes, onDrawerClose}) {
  return (
    <List className={classes.menuList}>
      <Link to="/folders" className={classes.foldersLink}>
        <ListItem
          button
          selected={true}
          onClick={onDrawerClose}
          classes={{
            root: classes.listItem,
            selected: classes.listItemSelected
          }}
        >
          <ListItemIcon>
            <StorageIcon/>
          </ListItemIcon>
          <ListItemText
            primaryTypographyProps={{
              classes: {
                root: classes.listTypography
              }
            }}
            primary="My files"
          />
        </ListItem>
      </Link>
    </List>
  )
}

export default withStyles(sideBarMenuStyles)(SideBarMenu);
