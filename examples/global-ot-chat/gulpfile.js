'use strict';

var gulp = require('gulp');
var sync = require('gulp-sync')(gulp).sync;
var shell = require('gulp-shell');

var jsTasks = require('./src/main/webapp/gulp/javascript');

// Javascript tasks
gulp.task('js:clean', jsTasks.jsClean);
gulp.task('js:bundle', jsTasks.createBundle);
gulp.task('rebundle', sync(['js:clean', 'js:bundle']));

// Java tasks
gulp.task('java:clean', shell.task(['mvn clean']));
gulp.task('java:test', shell.task(['mvn test']));
gulp.task('java:compile', shell.task(['mvn compile']));
gulp.task('java:package', shell.task(['mvn package']));
gulp.task('java:run:gateway', shell.task(['mvn exec:java -Dexec.mainClass=io.global.ot.chat.gateway.ChatGatewayLauncher']));
gulp.task('java:run:client', shell.task(['mvn exec:java -Dexec.mainClass=io.global.ot.chat.client.ChatClientLauncher']));

// General tasks
gulp.task('clean', ['js:clean', 'java:clean']);
gulp.task('test', ['java:test']);
gulp.task('build', sync(['clean', 'js:bundle', 'java:compile']));
gulp.task('pack', sync(['js:bundle', 'java:package']));
gulp.task('run:gateway', ['java:run:gateway']);
gulp.task('run:client', ['java:run:client']);

gulp.task('default', ['build']);
