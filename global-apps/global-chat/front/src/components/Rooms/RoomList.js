import React from "react";
import connectService from "../../common/connectService";
import RoomsContext from "../../modules/rooms/RoomsContext";
import CreateRoomForm from "./CreateRoomForm";
import RoomItem from "./RoomItem";

class RoomList extends React.Component {
  componentDidMount() {
    this.props.roomsService.init();
  }

  render() {
    return <div>Room List:
      {!this.props.roomsService.state.ready ?
        <p>Loading...</p> :
        <div>
          <ul>
            {this.props.roomsService.state.rooms.map(value => <RoomItem id={value.id} roomsService={this.props.roomsService}/>)}
          </ul>
          <CreateRoomForm roomsService={this.props.roomsService}/>
        </div>
      }
    </div>;
  }
}

export default connectService(RoomsContext, (state, roomsService) => ({roomsService}))(RoomList);
