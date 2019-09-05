// See https://developers.google.com/web/tools/workbox/guides/configure-workbox
workbox.setConfig({debug: true});

self.addEventListener('install', event => event.waitUntil(self.skipWaiting()));
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));

// We need this in Webpack plugin (refer to swSrc option): https://developers.google.com/web/tools/workbox/modules/workbox-webpack-plugin#full_injectmanifest_config
workbox.precaching.precacheAndRoute(self.__precacheManifest);

// Cache the Google static and apis. Using for Google Fonts
workbox.routing.registerRoute(
  /.*(?:googleapis|gstatic)\.com/,
  new workbox.strategies.StaleWhileRevalidate(),
);

// // Cache images
workbox.routing.registerRoute(
  /\.(?:png|gif|jpg|jpeg|svg)$/,
  new workbox.strategies.CacheFirst({
    cacheName: 'images',
    plugins: [
      new workbox.expiration.Plugin({
        maxEntries: 60,
        maxAgeSeconds: 30 * 24 * 60 * 60, // 30 Days
      }),
    ],
  })
);

// Cache API
workbox.routing.registerRoute(/\/ot\/.*$/, new workbox.strategies.NetworkFirst({
  cacheName: 'api',
  networkTimeoutSeconds: 1,
  plugins: [
    new workbox.expiration.Plugin({
      maxAgeSeconds: 60 * 60 * 24 * 7, // 1 Day
    }),
  ],
}));

self.addEventListener('sync', event => {
  console.log('sync!', event.tag);
    if (event.tag === 'firstTimeSync') {
    }
});
