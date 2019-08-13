import React from 'react';
import {Typography, withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../../common/checkAuth';
import ListContext from "../../modules/list/ListContext";
import {withSnackbar} from "notistack";
import TodoList from "../TodoList/TodoList";
import Container from '@material-ui/core/Container';
import ListService from "../../modules/list/ListService";
import Icon from '@material-ui/core/Icon';
import connectService from "../../common/connectService";
import AccountContext from "../../modules/account/AccountContext";

class MainScreen extends React.Component {
  constructor(props) {
    super(props);
    this.listService = ListService.create();
  }

  componentDidMount() {
    this.listService.init()
      .catch(err => {
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
        <div
          color="inherit"
          onClick={this.props.logout}
          className={this.props.classes.logout}
        >
          <Icon className={this.props.classes.logoutIcon} color="primary">logout</Icon>
        </div>
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
  connectService(
    AccountContext, (state, accountService) => ({
      logout() {
        accountService.logout();
      }
    })
  )(
    withSnackbar(
      withStyles(mainScreenStyles)(MainScreen)
    )
  )
);
