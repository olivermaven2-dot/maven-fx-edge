const CACHE="maven-fx-v9";
const ASSETS=["./","./index.html","./styles.css","./app.js","./manifest.webmanifest"];
self.addEventListener("install",e=>{self.skipWaiting();e.waitUntil(caches.open(CACHE).then(c=>c.addAll(ASSETS)))});
self.addEventListener("activate",e=>e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k!==CACHE).map(k=>caches.delete(k)))).then(()=>self.clients.claim())));
self.addEventListener("fetch",e=>{const u=new URL(e.request.url);if(u.pathname.endsWith("/app.js")||u.pathname.endsWith("/index.html")){e.respondWith(fetch(e.request,{cache:"no-store"}).then(r=>{const copy=r.clone();caches.open(CACHE).then(c=>c.put(e.request,copy));return r}).catch(()=>caches.match(e.request)));return}e.respondWith(caches.match(e.request).then(r=>r||fetch(e.request)))});
