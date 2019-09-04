import React from "react";
import {withStyles} from '@material-ui/core';
import Badge from "@material-ui/core/Badge";
import ListItemAvatar from "@material-ui/core/ListItemAvatar";
import MuiAvatar from "@material-ui/core/Avatar"
import avatarStyles from "./avatarStyles";
import {getAvatarLetters} from "..";

function Avatar({classes, name, selected}) {
  return (
    <ListItemAvatar className={classes.avatar}>
      <Badge
        invisible={selected}
        color="primary"
        variant="dot"
      >
        <MuiAvatar className={classes.avatarContent}>
          {getAvatarLetters(name)}
        </MuiAvatar>
      </Badge>
    </ListItemAvatar>
  );
}

export default withStyles(avatarStyles)(Avatar);

