import React from 'react';
import RoomList from "./Rooms/RoomList";
import ContactList from "./Contacts/ContactList";

class MainScreen extends React.Component {
  render() {
    return <div>
      <RoomList/>
      <ContactList/>
    </div>;
  }
}

export default MainScreen;
