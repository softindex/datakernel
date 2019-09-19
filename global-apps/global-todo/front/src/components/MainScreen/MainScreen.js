import React from 'react';
import {Typography, withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import TodoList from "../TodoList/TodoList";
import MainLayout from "../MainLayout/MainLayout";

function MainScreen({classes}) {
  return (
    <MainLayout>
      <Typography
        variant="h3"
        className={classes.title}
        color="primary"
      >
        Global Todo
      </Typography>
      <TodoList/>
      <Typography
        align="center"
        variant="subtitle2"
        className={classes.caption}
        color="textSecondary"
      >
        Click to edit Todo <br/>
        Created by SoftIndex LLC, 2019<br/>
        All rights reserved
      </Typography>
    </MainLayout>
  );
}

export default withStyles(mainScreenStyles)(MainScreen);
