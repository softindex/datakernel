const path = require('path');
const webpack = require('webpack');

module.exports = {
  entry: './src/common/GlobalFS.js',
  output: {
    path: path.resolve(__dirname, 'build'),
    filename: 'GlobalFS.js',
    library: 'GlobalFS',
    libraryTarget: 'umd',
    globalObject: `(typeof self !== 'undefined' ? self : self)`
  },
  optimization: {
    minimize: false
  }
};
