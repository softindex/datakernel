import React from 'react';
import Header from "./Header/Header"
import SideBar from "./SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../../common/checkAuth';
import ListsContext from "../../modules/lists/ListsContext";
import {withSnackbar} from "notistack";
import ListsService from "../../modules/lists/ListsService";
import EmptyList from "./EmptyList/EmptyList";
import ListServiceProvider from "../../modules/lists/ListServiceProvider";
import TodoList from "./TodoList/TodoList";

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
    const {lists, newLists} = this.listsService.state;
    const {history, match, classes} = this.props;
    const {listId} = this.props.match.params;
    return (
      <ListsContext.Provider value={this.listsService}>
        <Header listId={listId} listName={lists[listId]}/>
        <div className={classes.todoList}>
        <SideBar
          listId={listId}
          history={history}
          match={match}
        />
          {!listId && (
            <EmptyList/>
          )}
          {listId && (
            <ListServiceProvider
              listId={listId}
              isNew={newLists.has(listId)}>
              <TodoList/>
            </ListServiceProvider>
          )}
        </div>
      </ListsContext.Provider>
    );
  }
}

export default checkAuth(
  withSnackbar(
    withStyles(mainScreenStyles)(MainScreen)
  )
);
