import ListContext from "../../modules/list/ListContext";
import connectService from "../../common/connectService";
import todoListStyles from "./todoListStyles";
import * as React from "react";
import withStyles from "@material-ui/core/es/styles/withStyles";
import TextField from "@material-ui/core/TextField";
import TodoItem from "../TodoItem/TodoItem";
import {Paper, Button} from "@material-ui/core";
import Typography from "@material-ui/core/Typography";
import DoneAllIcon from '@material-ui/icons/DoneAll';

class TodoList extends React.Component {
  state = {
    newItemName: '',
    doneAll: false,
    selected: 'all'
  };

  onSubmit = e => {
    e.preventDefault();
    this.props.onCreateItem(this.state.newItemName);
    this.setState({newItemName: ''});
  };

  onItemChange = e => {
    this.setState({
      newItemName: e.target.value
    });
  };

  toggleDoneAll = () => {
    this.props.onToggleAllItemsStatus();
  };

  getAmountUncompletedTodo() {
    let counter = 0;
    Object.entries(this.props.items).map(([, isDone]) => {
      if (!isDone) {
        counter++;
      }
    });
    return counter;
  }

  onClearCompleted = () => {
    Object.entries(this.props.items).map(([name, isDone]) => {
      if (isDone) {
        return this.props.onDeleteItem(name);
      }
    })
  };

  onSelectedChange(selected) {
    this.setState({selected});
  }

  getFilteredTodo() {
    return Object.entries(this.props.items).filter(([, isDone]) => {
      if (this.state.selected === 'active') {
        return isDone === false;
      }
      if (this.state.selected === 'completed') {
        return isDone === true;
      }
      return true;
    })
  }

  render() {
    const {classes} = this.props;
    return (
      <Paper className={classes.paper}>
        <form onSubmit={this.onSubmit}>
          <TextField
            className={classes.textField}
            autoFocus
            placeholder="What needs to be done?"
            value={this.state.newItemName}
            onChange={this.onItemChange}
            variant="outlined"
            InputProps={{
              classes: {
                root: this.props.classes.itemInput,
              },
              startAdornment: (
                <div className={classes.iconButton} onClick={this.toggleDoneAll}>
                  <DoneAllIcon/>
                </div>
              )
            }}
          />
        </form>
        {this.getFilteredTodo(this.state.selected).map(([itemName, isDone]) => (
            <TodoItem
              name={itemName}
              isDone={isDone}
              onToggleItemStatus={this.props.onToggleItemStatus}
              onDeleteItem={this.props.onDeleteItem}
              onRenameItem={this.props.onRenameItem}
            />
          )
        )}
        {Object.entries(this.props.items).length !== 0 && (
          <div className={classes.listCaption}>
            <Typography
              variant="subtitle2"
              className={classes.captionCounter}
              color="textSecondary"
            >
              {this.getAmountUncompletedTodo()} items left
            </Typography>
            <Button
              className={classes.captionButton}
              variant={this.state.selected === 'all' ? "outlined" : null}
              onClick={this.onSelectedChange.bind(this, 'all')}
            >
              All
            </Button>
            <Button
              className={classes.captionButton}
              variant={this.state.selected === 'active' ? "outlined" : null}
              onClick={this.onSelectedChange.bind(this, 'active')}
            >
              Active
            </Button>
            <Button
              className={classes.captionButton}
              variant={this.state.selected === 'completed' ? "outlined" : null}
              onClick={this.onSelectedChange.bind(this, 'completed')}
            >
              Completed
            </Button>
            <Button className={classes.captionButton} onClick={this.onClearCompleted}>Clear completed</Button>
          </div>
        )}
      </Paper>
    );
  }
}

export default connectService(ListContext,
  ({items, ready}, listService) => ({
    ready,
    items,

    onCreateItem(name) {
      listService.createItem(name);
    },

    onDeleteItem(name) {
      listService.deleteItem(name);
    },

    onRenameItem(name, newName) {
      listService.renameItem(name, newName);
    },

    onToggleItemStatus(name) {
      listService.toggleItemStatus(name);
    },

    onToggleAllItemsStatus() {
      listService.toggleAllItemsStatus();
    }

  }))(
  withStyles(todoListStyles)(TodoList)
);
