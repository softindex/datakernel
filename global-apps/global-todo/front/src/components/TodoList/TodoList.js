import todoListStyles from "./todoListStyles";
import * as React from "react";
import withStyles from "@material-ui/core/es/styles/withStyles";
import TextField from "@material-ui/core/TextField";
import TodoItem from "../TodoItem/TodoItem";
import {Paper} from "@material-ui/core";
import DoneAllIcon from '@material-ui/icons/DoneAll';
import {getInstance, useService} from "global-apps-common";
import ListService from "../../modules/list/ListService";
import {useState} from "react";
import TodoFooter from "../TodoFooter/TodoFooter";
import {withSnackbar} from "notistack";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

function TodoListView({
                        classes,
                        items,
                        ready,
                        selected,
                        loading,
                        onSubmit,
                        newItemName,
                        onItemChange,
                        onToggleAllItemsStatus,
                        onToggleItemStatus,
                        onDeleteItem,
                        onRenameItem,
                        getFilteredTodo,
                        getAmountUncompletedTodo,
                        onSelectedChange,
                        onClearCompleted,
                      }) {
  return (
    <>
      {!ready && (
        <div className={classes.progressWrapper}>
          <CircularProgress/>
        </div>
      )}
      {ready && (
        <Paper className={classes.paper}>
          <form onSubmit={onSubmit}>
            <TextField
              className={classes.textField}
              autoFocus
              placeholder="What needs to be done?"
              value={newItemName}
              onChange={onItemChange}
              variant="outlined"
              InputProps={{
                classes: {
                  root: classes.itemInput,
                },
                startAdornment: (
                  <div className={classes.iconButton} onClick={onToggleAllItemsStatus}>
                    <DoneAllIcon/>
                  </div>
                ),
                endAdornment: (
                  <Grow in={loading}>
                    <div className={classes.progressWrapper}>
                      <CircularProgress size={28}/>
                    </div>
                  </Grow>
                )
              }}
            />
          </form>
          {getFilteredTodo(selected).map(([itemName, isDone]) => (
              <TodoItem
                name={itemName}
                isDone={isDone}
                onToggleItemStatus={onToggleItemStatus}
                onDeleteItem={onDeleteItem}
                onRenameItem={onRenameItem}
              />
            )
          )}
          <TodoFooter
            items={items}
            selected={selected}
            getAmountUncompletedTodo={getAmountUncompletedTodo}
            onSelectedChange={onSelectedChange}
            onClearCompleted={onClearCompleted}
          />
        </Paper>
      )}
    </>
  );
}

function TodoList({classes, enqueueSnackbar}) {
  const [newItemName, setNewItemName] = useState('');
  const [selected, setSelected] = useState('all');
  const [loading, setLoading] = useState(false);
  const listService = getInstance(ListService);
  const {items, ready} = useService(listService);

  function errorHandler(err) {
    enqueueSnackbar(err.message, {
      variant: 'error'
    });
  }

  const props = {
    items,
    ready,
    classes,
    selected,
    loading,
    newItemName,

    onDeleteItem(name) {
      listService.deleteItem(name).catch(errorHandler);
    },

    onRenameItem(name, newName) {
      listService.renameItem(name, newName).catch(errorHandler);
    },

    onToggleItemStatus(name) {
      listService.toggleItemStatus(name).catch(errorHandler);
    },

    onSubmit(e) {
      e.preventDefault();
      setLoading(true);
      listService.createItem(newItemName)
        .catch(errorHandler)
        .finally(() => {
          setNewItemName('');
          setLoading(false);
        })
    },

    onItemChange(e) {
      setNewItemName(e.target.value);
    },

    onToggleAllItemsStatus() {
      listService.toggleAllItemsStatus().catch(errorHandler);
    },

    getAmountUncompletedTodo() {
      let counter = 0;
      Object.entries(items).map(([, isDone]) => {
        if (!isDone) {
          counter++;
        }
      });
      return counter;
    },

    onClearCompleted() {
      Object.entries(items).map(([name, isDone]) => {
        if (isDone) {
          return props.onDeleteItem(name);
        }
      })
    },

    onSelectedChange(selected) {
      setSelected(selected);
    },

    getFilteredTodo() {
      return Object.entries(items)
        .sort(([leftName], [rightName]) => (
          Number(rightName.split(';')[0]) - Number(leftName.split(';')[0])
        ))
        .filter(([, isDone]) => {
        if (selected === 'active') {
          return isDone === false;
        }
        if (selected === 'completed') {
          return isDone === true;
        }
        return true;
      })
    }
  };

  return <TodoListView {...props}/>
}

export default withSnackbar(withStyles(todoListStyles)(TodoList));
