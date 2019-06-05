'use strict';

const gulp = require('gulp');
const shell = require('gulp-shell');

const jsTasks = require('./webapp/gulp/javascript');

// Javascript tasks
gulp.task('js:clean', jsTasks.jsClean);
gulp.task('js:bundle', jsTasks.createBundle);

// General tasks
gulp.task('clean', gulp.parallel('js:clean'));
gulp.task('build', gulp.series('js:clean', 'js:bundle'));

gulp.task('default', gulp.series('build'));
