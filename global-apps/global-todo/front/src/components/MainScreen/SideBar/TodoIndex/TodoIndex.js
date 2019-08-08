import React from "react";
import path from "path";
import {withStyles} from '@material-ui/core';
import TodoIndexItem from "./TodoIndexItem/TodoIndexItem";
import todoIndexStyles from "./todoIndexStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

class TodoIndex extends React.Component {
  getListPath = (noteId) => {
    return path.join('/', noteId || '');
  };

  render() {
    const {classes, ready, lists} = this.props;
    return (
      <>
        {!ready && (
          <Grow in={!ready}>
            <div className={classes.progressWrapper}>
              <CircularProgress/>
            </div>
          </Grow>
        )}
        {ready && (
          <div className={classes.listsList}>
            <List>
              {Object.entries(lists).map(([listId, listName], index) =>
                (
                  <TodoIndexItem
                    key={index}
                    listId={listId}
                    listName={listName}
                    getListPath={this.getListPath}
                    showMenuIcon={true}
                    onRename={this.props.onRename}
                    onDelete={this.props.onDelete}
                  />
                )
              )}
            </List>
          </div>
        )}
      </>
    );
  }
}

export default withStyles(todoIndexStyles)(TodoIndex);
