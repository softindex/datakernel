'use strict';

const gulp = require('gulp');
const shell = require('gulp-shell');

const browserify = require('browserify');
const del = require('del');
const fs = require('fs');

const SRC_PATH = './src/main/webapp/src';
const BUNDLE_PATH = './src/main/resources/static/js/bundle.js';

function bundle() {
  return browserify(SRC_PATH).transform('babelify').bundle().pipe(fs.createWriteStream(BUNDLE_PATH));
}

// Javascript tasks
gulp.task('js:clean', () => del(BUNDLE_PATH));
gulp.task('js:bundle', bundle);

// Java tasks
gulp.task('java:clean', shell.task(['mvn clean']));
gulp.task('java:test', shell.task(['mvn test']));
gulp.task('java:compile', shell.task(['mvn compile']));
gulp.task('java:package', shell.task(['mvn package']));
gulp.task('java:run', shell.task(['mvn exec:java -Dexec.mainClass=io.global.fs.demo.GlobalFsDemoApp']));

// General tasks
gulp.task('clean', gulp.parallel('js:clean', 'java:clean'));
gulp.task('test', gulp.series('java:test'));
gulp.task('build', gulp.series('clean', 'js:bundle', 'java:compile'));
gulp.task('pack', gulp.series('js:bundle', 'java:package'));
gulp.task('run', gulp.series('java:run'));

gulp.task('watch', () =>
  gulp.watch(`${SRC_PATH}/*.js`).on('change', file => {
    console.log(`reloading ${file}`);
    bundle();
  }));

gulp.task('default', gulp.series('build', 'java:run'));
