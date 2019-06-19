import React from "react";
import path from "path";

class RoomItem extends React.Component {
  constructor(props){
    super(props);
    this.quitRoom = this.quitRoom.bind(this);
  }

  quitRoom(){
    this.props.roomsService.quitRoom(this.props.id);
  }

  render() {
    let {id} = this.props;

    return <li>
      <a href={path.join('/room', id)}>
        {id.substring(0, 8)}
      </a>
      <button onClick={this.quitRoom}>
        Quit
      </button>
    </li>;
  }
}

export default RoomItem;
