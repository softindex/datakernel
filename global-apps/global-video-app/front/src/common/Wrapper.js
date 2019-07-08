import React from 'react';

export default ({children, ...commonProps}) => {
  return React.Children.map(children, child => React.cloneElement(child, commonProps));
}

