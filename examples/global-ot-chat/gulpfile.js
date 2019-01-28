'use strict';

const gulp = require('gulp');
const shell = require('gulp-shell');

const jsTasks = require('./src/main/webapp/gulp/javascript');

// Javascript tasks
gulp.task('js:clean', jsTasks.jsClean);
gulp.task('js:bundle', jsTasks.createBundle);
gulp.task('rebundle', gulp.series('js:clean', 'js:bundle'));

// Java tasks
gulp.task('java:clean', shell.task(['mvn clean']));
gulp.task('java:test', shell.task(['mvn test']));
gulp.task('java:compile', shell.task(['mvn compile']));
gulp.task('java:package', shell.task(['mvn package']));
gulp.task('java:run', shell.task(['mvn exec:java -Dexec.mainClass=io.global.ot.chat.client.GlobalChatDemoApp']));

// General tasks
gulp.task('clean', gulp.parallel('js:clean', 'java:clean'));
gulp.task('test', gulp.series('java:test'));
gulp.task('build', gulp.series('clean', 'js:bundle', 'java:compile'));
gulp.task('pack', gulp.parallel('js:bundle', 'java:package'));
gulp.task('run', gulp.series('java:run'));

gulp.task('default', gulp.series('build', 'run'));
