const $ = require('jquery');

if (location.pathname === '/') {
  $(() => require('./login'));
} else if (location.pathname === '/announce') {
  $(() => require('./announcement'));
} else {
  $(() => require('./repository'));
}
