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
  constructor(props) {
    super(props);
    this.state = {
      newItemName: '',
      selected: {
        allSelected: true,
        activeSelected: false,
        completedSelected: false
      }
    };
  }

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

  getAmountUncompletedTodo() {
    let counter = 0;
    Object.entries(this.props.items).map(([, isDone]) => {
      if (!isDone) {
        counter++;
      }
    });
    return counter;
  }

  onAllSelected = () => {
    this.setState({
      selected: {
        allSelected: true,
        activeSelected: false,
        completedSelected: false
      }
    });
  };

  onActiveSelected = () => {
    this.setState({
      selected: {
        allSelected: false,
        activeSelected: true,
        completedSelected: false
      }
    });
  };

  onCompletedSelected = () => {
    this.setState({
      selected: {
        allSelected: false,
        activeSelected: false,
        completedSelected: true
      }
    });
  };

  onClearCompleted = () => {
    Object.entries(this.props.items).map(([name, isDone]) => {
      if (isDone) {
        return this.props.onDeleteItem(name);
      }
    })
  };

  getFilteredTodo() {
    return Object.entries(this.props.items).filter(([, isDone]) => {
      if (this.state.selected.activeSelected) {
        return isDone === false;
      }
      if (this.state.selected.completedSelected) {
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
                <div className={classes.iconButton}>
                  <DoneAllIcon/>
                </div>
              )
            }}
          />
        </form>
        {this.getFilteredTodo().map(([itemName, isDone]) => (
            <TodoItem
              name={itemName}
              isDone={isDone}
              doneAll={this.state.doneAll}
              onChangeItemState={this.props.onChangeItemState}
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
              variant={this.state.selected.allSelected ? "outlined" : null}
              onClick={this.onAllSelected}
            >
              All
            </Button>
            <Button
              className={classes.captionButton}
              variant={this.state.selected.activeSelected ? "outlined" : null}
              onClick={this.onActiveSelected}
            >
              Active
            </Button>
            <Button
              className={classes.captionButton}
              onClick={this.onCompletedSelected}
              variant={this.state.selected.completedSelected ? "outlined" : null}
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

    onChangeItemState(name) {
      listService.changeItemState(name);
    }

  }))(
  withStyles(todoListStyles)(TodoList)
);
