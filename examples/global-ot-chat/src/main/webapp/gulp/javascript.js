'use strict';

var gulp = require('gulp');
var browserify = require('browserify');
var fs = require('fs');
var del = require('del');
var rename = require('gulp-rename')

var BUNDLE_PATH = './src/main/resources/static/js/bundle.js';

function createBundle() {
	return browserify('./src/main/webapp/src')
        .transform('babelify', {presets: ['react']})
        .bundle()
        .pipe(fs.createWriteStream(BUNDLE_PATH));
}

function jsClean() {
    return del(BUNDLE_PATH);
}

module.exports = {
	createBundle: createBundle
};
