import React from "react";
import {withStyles} from '@material-ui/core';
import contactChipStyles from './contactChipStyles'
import {getAvatarLetters} from "global-apps-common";
import Avatar from "@material-ui/core/Avatar";
import MuiChip from "@material-ui/core/Chip";

function ContactChipComponent({classes, ...otherProps}) {
  return (
    <MuiChip
      {...otherProps}
      className={classes.chip}
      color="primary"
      avatar={
        <Avatar>
          {getAvatarLetters(otherProps.label)}
        </Avatar>
      }
      classes={{
        label: classes.chipText
      }}
    />
  );
}

const ContactChip = withStyles(contactChipStyles)(ContactChipComponent);

export {ContactChip};
