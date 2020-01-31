self.importScripts('https://cdnjs.cloudflare.com/ajax/libs/localforage/1.7.3/localforage.min.js', 'GlobalFS.js');
// See https://developers.google.com/web/tools/workbox/guides/configure-workbox
workbox.core.setLogLevel(workbox.core.LOG_LEVELS.debug);

self.addEventListener('install', event => event.waitUntil(self.skipWaiting()));
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));

// We need this in Webpack plugin (refer to swSrc option): https://developers.google.com/web/tools/workbox/modules/workbox-webpack-plugin#full_injectmanifest_config
workbox.precaching.precacheAndRoute(self.__precacheManifest);

workbox.routing.registerNavigationRoute('/index.html', {
  blacklist: [/\/debug/]
});

// Cache the Google static and apis. Using for Google Fonts
workbox.routing.registerRoute(
  /.*(?:googleapis|gstatic)\.com/,
  workbox.strategies.staleWhileRevalidate(),
);

// Cache images
workbox.routing.registerRoute(
  /\.(?:png|gif|jpg|jpeg|svg)$/,
  workbox.strategies.cacheFirst({
    cacheName: 'images',
    plugins: [
      new workbox.expiration.Plugin({
        maxEntries: 60,
        maxAgeSeconds: 30 * 24 * 60 * 60, // 30 Days
      }),
    ],
  })
);

// Cache GlobalFS API
workbox.routing.registerRoute(/\/list\/.*$/, workbox.strategies.networkFirst({
  cacheName: 'api'
}));
workbox.routing.registerRoute(/\/download\/.*$/, workbox.strategies.networkFirst({
  cacheName: 'api',
  plugins: [
    new workbox.expiration.Plugin({
      maxEntries: 60,
      maxAgeSeconds: 30 * 24 * 60 * 60, // 30 Days
    }),
  ],
}));

const globalFS = new GlobalFS.default();

localforage.config({
  driver: localforage.INDEXEDDB,
  name: 'OperationsStore',
  storeName: 'OperationsStore'
});

// custom code
self.addEventListener('sync', async event => {
  if (event.tag === 'connectionExists') {
    event.waitUntil(localforage.iterate((value, key) => {
      postOperation(key, value);
    }));
  }
});

globalFS.addListener('progress', ({progress, fileName}) => {
  const channel = new BroadcastChannel('progress');
  channel.postMessage({
    fileName,
    progress
  });
});

const postOperation = async (fullName, operation) => {
  switch (operation.type) {
    case 'UPLOAD': {
      const index = fullName.lastIndexOf('/');
      await globalFS.upload(index !== -1 ? fullName.slice(0, index) : '/', operation.payload);
      await localforage.removeItem(fullName);
      break;
    }

    case 'DELETE_FOLDER':
    case 'DELETE': {
      await globalFS.remove(fullName);
      await localforage.removeItem(fullName);
      break;
    }
  }
};


