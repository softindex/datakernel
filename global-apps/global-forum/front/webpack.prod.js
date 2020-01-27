const path = require('path');
const merge = require('webpack-merge');

const TerserJSPlugin = require('terser-webpack-plugin');
const OptimizeCssAssetsPlugin = require('optimize-css-assets-webpack-plugin');

module.exports = merge(require('./webpack.config.js'), {
  mode: 'production',
  devtool: false,
  plugins: [
    new TerserJSPlugin(),
    new OptimizeCssAssetsPlugin()
  ],
  output: {
    path: path.resolve(__dirname, 'build')
  }
});
