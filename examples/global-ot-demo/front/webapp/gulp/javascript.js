'use strict';

const gulp = require('gulp');
const browserify = require('browserify');
const fs = require('fs');
const del = require('del');

const SRC_PATH = 'webapp/src';
const BUNDLE_PATH = 'build/js/bundle.js';

function createBundle() {
	return browserify(SRC_PATH)
        .transform('babelify')
        .bundle()
        .pipe(fs.createWriteStream(BUNDLE_PATH));
}

function jsClean() {
    return del(BUNDLE_PATH);
}

module.exports = {
	createBundle: createBundle,
	jsClean: jsClean
};
