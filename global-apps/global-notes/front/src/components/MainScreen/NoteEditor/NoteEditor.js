import React from 'react';
import {getDifference} from './utils';
import connectService from "../../../common/connectService";
import NoteContext from "../../../modules/note/NoteContext";

class NoteEditor extends React.Component {
  onChange = (event) => {
    const difference = getDifference(this.props.content, event.target.value, event.target.selectionEnd);
    if (!difference) {
      return;
    }

    switch (difference.operation) {
      case 'insert':
        this.props.onInsert(difference.position, difference.content);
        break;
      case 'delete':
        this.props.onDelete(difference.position, difference.content);
        break;
      case 'replace':
        this.props.onReplace(difference.position, difference.oldContent, difference.newContent);
        break;
      default:
        throw new Error('Unsupported operation');
    }
  };

  render() {
    return (
      <textarea
        className={this.props.className}
        value={this.props.content}
        onChange={this.onChange}
      />
    );
  }
}

export default connectService(NoteContext, ({content}, noteService) => ({content, noteService}))(NoteEditor);
