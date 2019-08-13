import React from 'react';
import {Typography, withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../../common/checkAuth';
import ListContext from "../../modules/list/ListContext";
import {withSnackbar} from "notistack";
import TodoList from "../TodoList/TodoList";
import Container from '@material-ui/core/Container';
import ListService from "../../modules/list/ListService";

class MainScreen extends React.Component {
  constructor(props) {
    super(props);
    this.listService = ListService.create();
  }

  componentDidMount() {
    this.listService.init()
      .catch((err) => {
        this.props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      });
  }

  componentWillUnmount() {
    this.listService.stop();
  }

  render() {
    return (
      <ListContext.Provider value={this.listService}>
        <Container
          maxWidth="sm"
          className={this.props.classes.container}
        >
          <Typography
            variant="h3"
            className={this.props.classes.title}
            color="primary"
          >
            Global Todo
          </Typography>
          <TodoList/>
          <Typography
            align="center"
            variant="subtitle2"
            className={this.props.classes.caption}
            color="textSecondary"
          >
            Click to edit Todo <br/>
            Created by SoftIndex <br/>
            All rights reserved
          </Typography>
        </Container>
      </ListContext.Provider>
    );
  }
}

export default checkAuth(
  withSnackbar(
    withStyles(mainScreenStyles)(MainScreen)
  )
);
