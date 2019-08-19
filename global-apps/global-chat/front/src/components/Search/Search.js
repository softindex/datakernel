import React from "react";
import {withStyles} from '@material-ui/core';
import IconButton from "@material-ui/core/IconButton";
import SearchIcon from "@material-ui/icons/Search";
import InputBase from "@material-ui/core/InputBase";
import Paper from "@material-ui/core/Paper";
import Grow from "@material-ui/core/Grow";
import CircularProgress from "@material-ui/core/CircularProgress";
import searchStyles from "./searchStyles";


function Search({classes, placeholder, onChange, searchValue, searchReady}) {
  return (
    <Paper className={classes.search}>
      <IconButton
        className={classes.iconButton}
        disabled={true}
      >
        <SearchIcon/>
      </IconButton>
      <InputBase
        className={classes.inputDiv}
        placeholder={placeholder}
        autoFocus
        value={searchValue}
        onChange={onChange}
        classes={{input: classes.input}}
        endAdornment={
          <Grow in={searchReady === false && searchValue !== ''}>
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

