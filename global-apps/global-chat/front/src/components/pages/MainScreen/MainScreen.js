import React from 'react';
import Header from "../../Header/Header"
import ChatRoom from "../../ChatRoom/ChatRoom"
import SideBar from "./SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../../HOC/checkAuth';
import connectService from "../../../common/connectService";
import RoomsContext from "../../../modules/rooms/RoomsContext";
import ContactsContext from "../../../modules/contacts/ContactsContext";
import {withSnackbar} from "notistack";
import * as PropTypes from "prop-types";
import StartChat from "../../EmptyChatRoom/EmptyChatRoom";
import ContactsService from "../../../modules/contacts/ContactsService";
import RoomsService from "../../../modules/rooms/RoomsService";

class MainScreen extends React.Component {
  constructor(props) {
    super(props);
    this.contactsService = ContactsService.create();
    this.roomsService = RoomsService.createForm(this.contactsService);
  }

  componentDidMount() {
    Promise.all([
      this.roomsService.init(),
      this.contactsService.init()
    ]).catch((err) => {
      this.props.enqueueSnackbar(err.message, {
        variant: 'error'
      });
    });
  }

  componentWillUnmount() {
    this.roomsService.stop();
    this.contactsService.stop();
  }

  render() {
    const {roomId} = this.props.match.params;
    return (
      <RoomsContext.Provider value={this.roomsService}>
        <ContactsContext.Provider value={this.contactsService}>
          <Header/>
          <div className={this.props.classes.chat}>
            <SideBar/>
            {!roomId && (
              <StartChat/>
            )}
            {roomId && (
              <ChatRoom roomId={roomId}/>
            )}
          </div>
        </ContactsContext.Provider>
      </RoomsContext.Provider>
    );
  }
}

MainScreen.propTypes = {
  enqueueSnackbar: PropTypes.func.isRequired,
};

export default checkAuth(
  withSnackbar(withStyles(mainScreenStyles)(MainScreen))
);
