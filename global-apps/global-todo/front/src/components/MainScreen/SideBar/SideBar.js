import React from 'react';
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import Typography from '@material-ui/core/Typography';
import Paper from "@material-ui/core/Paper";
import Button from "@material-ui/core/Button";
import CreateListForm from "../DialogsForms/CreateListForm"
import TodoIndex from "./TodoIndex/TodoIndex";
import connectService from "../../../common/connectService";
import ListsContext from "../../../modules/lists/ListsContext";
import DeleteListForm from "../DialogsForms/DeleteListForm";
import RenameListForm from "../DialogsForms/RenameListForm";
import {Redirect} from "react-router-dom";

class SideBar extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      showCreateDialog: false,
      deleteDialog: {
        show: false,
        listId: null
      },
      renameDialog: {
        show: false,
        listId: null,
        listName: ''
      }
    }
  };

  showCreateDialog = () => this.setState({
    ...this.state,
    showCreateDialog: true
  });
  showDeleteDialog = (listId) => this.setState({
    ...this.state, deleteDialog: {
      show: true,
      listId
    }
  });
  showRenameDialog = (listId, listName) => this.setState({
    ...this.state, renameDialog: {
      show: true,
      listId,
      listName
    }
  });

  closeDialogs = () => {
    this.setState({
      showCreateDialog: false,
      deleteDialog: {
        show: false,
        listId: null
      },
      renameDialog: {
        show: false,
        listId: null,
        listName: ''
      }
    });
  };

  render() {
    const {classes, lists, ready, listId, history} = this.props;
    if (listId && ready && !lists[listId]) {
      return <Redirect to='/'/>;
    }
    return (
      <div className={classes.wrapper}>
        <Paper square className={classes.paper}/>
        <Typography
          className={classes.tabContent}
          component="div"
          style={{padding: 12}}
        >
          <CreateListForm
            history={history}
            open={this.state.showCreateDialog}
            onClose={this.closeDialogs}
            onCreate={this.props.onCreate}
          />
          <DeleteListForm
            open={this.state.deleteDialog.show}
            onClose={this.closeDialogs}
            onDelete={() => this.props.onDelete(this.state.deleteDialog.listId)}
          />
          <RenameListForm
            open={this.state.renameDialog.show}
            listName={this.state.renameDialog.listName}
            onClose={this.closeDialogs}
            onRename={newName => this.props.onRename(this.state.renameDialog.listId, newName)}
          />

          <Button
            className={classes.button}
            fullWidth={true}
            variant="contained"
            size="medium"
            color="primary"
            onClick={this.showCreateDialog}
          >
            New List
          </Button>
          <div className={classes.listsList}>
            <TodoIndex
              lists={lists}
              ready={ready}
              onRename={this.showRenameDialog}
              onDelete={this.showDeleteDialog}
            />
          </div>
        </Typography>
      </div>
    );
  }
}

export default connectService(
  ListsContext, ({ready, lists}, listsService) => ({
    ready,
    lists,
    onCreate(name) {
      return listsService.createList(name);
    },
    onRename(id, newName) {
      return listsService.renameList(id, newName);
    },
    onDelete(id) {
      return listsService.deleteList(id);
    }
  })
)(
  withStyles(sideBarStyles)(SideBar)
);
