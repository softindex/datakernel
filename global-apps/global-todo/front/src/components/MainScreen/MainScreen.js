import React from 'react';
import {Typography, withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../../common/checkAuth';
import ListsContext from "../../modules/lists/ListsContext";
import {withSnackbar} from "notistack";
import ListsService from "../../modules/lists/ListsService";
import ListServiceProvider from "../../modules/lists/ListServiceProvider";
import TodoList from "../TodoList/TodoList";
import Container from '@material-ui/core/Container';

class MainScreen extends React.Component {
  constructor(props) {
    super(props);
    this.listsService = ListsService.create();
  }

  componentDidMount() {
    this.listsService.init()
      .catch((err) => {
        this.props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      });
  }

  componentWillUnmount() {
    this.listsService.stop();
  }

  render() {
    return (
      <ListsContext.Provider value={this.listsService}>
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
          <ListServiceProvider
            listId={this.props.listId}
            isNew={this.listsService.state.newLists.has(this.props.listId)}>
            <TodoList/>
          </ListServiceProvider>
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
      </ListsContext.Provider>
    );
  }
}

export default checkAuth(
  withSnackbar(
    withStyles(mainScreenStyles)(MainScreen)
  )
);
