import React from "react";
import {withStyles} from '@material-ui/core';
import IconButton from "@material-ui/core/IconButton";
import SearchIcon from "@material-ui/icons/Search";
import InputBase from "@material-ui/core/InputBase";
import Paper from "@material-ui/core/Paper";
import Grow from "@material-ui/core/Grow";
import CircularProgress from "@material-ui/core/CircularProgress";
import searchStyles from "./searchStyles";

function Search({classes, searchReady, ...otherProps}) {
  return (
    <Paper className={classes.root}>
      <IconButton
        className={classes.iconButton}
        disabled={true}
      >
        <SearchIcon/>
      </IconButton>
      <InputBase
        {...otherProps}
        className={classes.inputDiv}
        classes={{input: classes.input}}
        autoFocus
        endAdornment={
          <Grow in={searchReady === false && otherProps.value !== '' && !otherProps.disabled}>
            <div className={classes.progressWrapper}>
              <CircularProgress size={24}/>
            </div>
          </Grow>
        }
      />
    </Paper>
  );
}

export default withStyles(searchStyles)(Search);
