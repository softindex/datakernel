import React from 'react';
import Header from "../../Header/Header"
import ChatRoom from "../../ChatRoom/ChatRoom"
import SideBar from "../../SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../../HOC/checkAuth';
import connectService from "../../../common/connectService";
import RoomsContext from "../../../modules/rooms/RoomsContext";
import ContactsContext from "../../../modules/contacts/ContactsContext";

class MainScreen extends React.Component {
  componentDidMount() {
    Promise.all([
      this.props.roomsService.init(),
      this.props.contactsService.init()
    ]).catch(err => alert(err.message));
  }

  render() {
    return (
      <>
        <Header/>
        <div className="chat">
          <SideBar/>
          <ChatRoom/>
        </div>
      </>
    );
  }
}

export default checkAuth(
  connectService(ContactsContext, (state, contactsService) => ({contactsService}))(
    connectService(RoomsContext, (state, roomsService) => ({roomsService}))(
      withStyles(mainScreenStyles)(MainScreen)
    )
  )
);
