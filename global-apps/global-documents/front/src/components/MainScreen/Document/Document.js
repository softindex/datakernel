import React from 'react';
import {withStyles} from '@material-ui/core';
import PropTypes from 'prop-types';
import documentStyles from './documentStyles';
import DocumentService from "../../../modules/document/DocumentService";
import DocumentContext from '../../../modules/document/DocumentContext';
import DocumentEditor from "../DocumentEditor/DocumentEditor";

class Document extends React.Component {
  static propTypes = {
    documentId: PropTypes.string.isRequired
  };

  state = {
    documentId: null,
    documentService: null
  };

  onInsert = (position, content) => {
    this.state.documentService.insert(position, content);
  };

  onDelete = (position, content) => {
    this.state.documentService.delete(position, content);
  };

  onReplace = (position, oldContent, newContent) => {
    this.state.documentService.replace(position, oldContent, newContent);
  };

  onRename = (newName) => {
    this.state.documentService.rename(newName);
  };

  static getDerivedStateFromProps(props, state) {
    if (props.documentId !== state.documentId) {
      if (state.documentService) {
        state.documentService.stop();
      }

      const documentService = DocumentService.createFrom(props.documentId, props.isNew);
      documentService.init();

      return {
        documentId: props.documentId,
        documentService
      };
    }
    return state;
  }

  componentWillUnmount() {
    this.state.documentService.stop();
  }

  update = newState => this.setState(newState);

  render() {
    const {classes} = this.props;
    return (
      <DocumentContext.Provider value={this.state.documentService}>
        <div className={classes.root}>
          <div className={classes.headerPadding}/>
          <DocumentEditor
            onInsert={this.onInsert}
            onDelete={this.onDelete}
            onReplace={this.onReplace}
          />
        </div>
      </DocumentContext.Provider>
    );
  }
}

export default withStyles(documentStyles)(Document);
