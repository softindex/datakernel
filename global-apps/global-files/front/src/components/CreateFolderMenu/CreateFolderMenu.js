import MenuList from "@material-ui/core/MenuList";
import MenuItem from "@material-ui/core/MenuItem";
import ListItemIcon from "@material-ui/core/ListItemIcon";
import CreateFolderIcon from '@material-ui/icons/CreateNewFolderOutlined';
import ListItemText from "@material-ui/core/ListItemText";
import Divider from "@material-ui/core/Divider";
import Menu from "@material-ui/core/Menu";
import FileIcon from '@material-ui/icons/InsertDriveFileOutlined';
import React from "react";
import {withStyles} from "@material-ui/core";
import createFolderMenuStyles from "./createFolderMenuStyles";

function CreateFolderMenu({classes, fabElement, onCloseFabMenu, onDialogOpen, onFileUpload, inputRef}) {
  return (
    <Menu
      open={Boolean(fabElement)}
      onClose={onCloseFabMenu}
      disableAutoFocusItem
      anchorEl={fabElement}
    >
      <MenuList>
        <MenuItem onClick={onDialogOpen}>
          <ListItemIcon className={classes.listItemIcon}>
            <CreateFolderIcon/>
          </ListItemIcon>
          <ListItemText
            className={classes.litItemText}
            primary="Create folder"
          />
        </MenuItem>
        <Divider/>
        <input
          onChange={onFileUpload}
          ref={inputRef}
          multiple type="file"
          id="file"
          className={classes.uploadInput}
        />
        <label htmlFor="file">
          <MenuItem>
            <ListItemIcon className={classes.listItemIcon}>
              <FileIcon/>
            </ListItemIcon>
            <ListItemText
              className={classes.litItemText}
              primary="Upload File"
            />
          </MenuItem>
        </label>
      </MenuList>
    </Menu>
  )
}

export default withStyles(createFolderMenuStyles)(CreateFolderMenu);
