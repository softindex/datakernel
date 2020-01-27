import 'bootstrap/js/dist/modal';
import 'bootstrap/js/dist/dropdown';
import 'bootstrap/js/dist/popover';

import 'bootstrap/dist/css/bootstrap.min.css';

import 'tempusdominus-bootstrap-4/build/js/tempusdominus-bootstrap-4.min';
import 'tempusdominus-bootstrap-4/build/css/tempusdominus-bootstrap-4.min.css';

import '@fortawesome/fontawesome-free/js/fontawesome';
import '@fortawesome/fontawesome-free/js/solid';

import {setupBlogPost} from './blog';

import setup from 'global-comm/src/setups';
import setupAllPosts from 'global-comm/src/posts';

import './style.css';

import $ from 'jquery';

setupBlogPost($('#root-post'));

setup();
setupAllPosts();

