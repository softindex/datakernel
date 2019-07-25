import ListService from "../list/ListService";
import ListContext from "../list/ListContext";
import * as React from "react";
import * as PropTypes from "prop-types";

class ListServiceProvider extends React.Component {
  static propTypes = {
    listId: PropTypes.string.isRequired
  };

  state = {
    listId: null,
    listService: null
  };

  static getDerivedStateFromProps(props, state) {
    if (props.listId !== state.listId) {
      if (state.listService) {
        state.listService.stop();
      }

      const listService = ListService.createFrom(props.listId, props.isNew);
      listService.init();

      return {
        listId: props.listId,
        listService
      };
    }
  }

  componentWillUnmount() {
    this.state.listService.stop();
  }

  update = newState => this.setState(newState);

  render() {
    return (
      <ListContext.Provider value={this.state.listService}>
        {this.props.children}
      </ListContext.Provider>
    );
  }
}

export default ListServiceProvider;
