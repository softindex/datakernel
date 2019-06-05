import _ from 'lodash';
import React from 'react';
import {getDifference} from './utils';

class TextArea extends React.Component {
  onChange = (event) => {
    const difference = getDifference(this.props.value, event.target.value, event.target.selectionEnd);
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
        {..._.omit(this.props, ['onInsert', 'onDelete', 'onReplace'])}
        onChange={this.onChange}
      />
    );
  }
}

export default TextArea;
