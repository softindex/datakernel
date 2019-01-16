const $ = require('jquery');

if (location.pathname === '/') {
  $(() => require('./login'));
} else {
  $(() => require('./repository'));
}
