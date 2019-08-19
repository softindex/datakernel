import React from "react";
import {withStyles} from '@material-ui/core';
import chipStyles from './chipStyles'
import {getAppStoreContactName, getAvatarLetters} from "../../common/utils";
import Avatar from "@material-ui/core/Avatar";
import MuiChip from "@material-ui/core/Chip";

function Chip({classes, names, pubKey, searchContacts, loading, onContactCheck}) {
  return (
    <MuiChip
      color="primary"
      label={names.get(pubKey) || getAppStoreContactName(searchContacts.get(pubKey))}
      avatar={
        <Avatar>
          {getAvatarLetters(names.get(pubKey)) ||
          getAvatarLetters(getAppStoreContactName(searchContacts.get(pubKey)))}
        </Avatar>
      }
      onDelete={!loading && onContactCheck}
      className={classes.chip}
      classes={{
        label: classes.chipText
      }}
    />
  );
}

export default withStyles(chipStyles)(Chip);

