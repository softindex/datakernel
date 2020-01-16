import React from 'react';
import {checkAuth, AuthContext, connectService, useService, getInstance} from 'global-apps-common';
import RoomsService from "../../modules/rooms/RoomsService";
import NamesService from "../../modules/names/NamesService";
import {getRoomName} from "../../common/utils";
import Header from "../Header/Header";
import {withRouter} from "react-router-dom";

function MainLayout({publicKey, match, children}) {
  const roomsService = getInstance(RoomsService);
  const namesService = getInstance(NamesService);
  const {rooms} = useService(roomsService);
  const {names} = useService(namesService);
  const {roomId} = match.params;

  return (
    <>
      <Header
        roomId={roomId}
        title={rooms.has(roomId) ?
          getRoomName(
            rooms.get(roomId).participants,
            names, publicKey
          ) : ''
        }
      />
      {children}
    </>
  );
}

export default connectService(
  AuthContext, ({publicKey}) => ({publicKey})
)(
  checkAuth(
    withRouter(MainLayout)
  )
);
