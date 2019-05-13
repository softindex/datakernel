'use strict';

var gulp = require('gulp');
var sync = require('gulp-sync')(gulp).sync;
var shell = require('gulp-shell');

var jsTasks = require('./src/main/webapp/gulp/javascript');

// Javascript tasks
gulp.task('js:clean', jsTasks.jsClean);
gulp.task('js:bundle', jsTasks.createBundle);
gulp.task('js:style', jsTasks.copyLess);

// Java tasks
gulp.task('java:clean', shell.task(['mvn clean']));
gulp.task('java:test', shell.task(['mvn test']));
gulp.task('java:compile', shell.task(['mvn compile']));
gulp.task('java:package', shell.task(['mvn package']));
gulp.task('java:run', shell.task(['mvn exec:java -Dexec.mainClass=io.datakernel.examples.UIKernelWebAppLauncher']));

// General tasks
gulp.task('clean', ['js:clean', 'java:clean']);
gulp.task('test', ['java:test']);
gulp.task('build', sync(['clean', 'js:bundle', 'js:style', 'java:compile']));
gulp.task('pack', sync(['js:bundle', 'java:package']));
gulp.task('run', ['java:run']);

gulp.task('default', sync(['build', 'java:run']));
