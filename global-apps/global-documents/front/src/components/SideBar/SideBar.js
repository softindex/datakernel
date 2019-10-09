import React, {useState} from 'react';
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import Typography from '@material-ui/core/Typography';
import DocumentsList from "../DocumentsList/DocumentsList";
import {getInstance, useService} from "global-apps-common";
import DocumentsService from "../../modules/documents/DocumentsService";
import Search from "../Search/Search";

function SideBarView({classes, documents, documentsReady, search, onSearchChange}) {
  return (
    <div className={classes.wrapper}>
      <Search
        classes={{root: classes.search}}
        placeholder="Documents..."
        value={search}
        onChange={onSearchChange}
      />
      <div className={`${classes.documentsList} scroller`}>
        <DocumentsList
          documents={documents}
          ready={documentsReady}
        />
        {documents.size === 0 && search !== '' && (
          <Typography
            className={classes.secondaryText}
            color="textSecondary"
            variant="body1"
          >
            Nothing found
          </Typography>
        )}
      </div>
    </div>
  );
}

function SideBar({classes}) {
  const documentsService = getInstance(DocumentsService);
  const {documents, documentsReady} = useService(documentsService);
  const [search, setSearch] = useState('');
  const props = {
    classes,
    search,
    documentsReady,

    onSearchChange(event) {
      setSearch(event.target.value)
    },

    documents: new Map([...documents]
      .filter(([, {name}]) => name
        .toLowerCase()
        .includes(search.toLowerCase())))
  };

  return <SideBarView {...props}/>
}

export default withStyles(sideBarStyles)(SideBar);
