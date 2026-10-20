// Bench-only: make Node's DNS *resolver* (dns.promises.Resolver#resolve4/resolve6) honour /etc/hosts.
// Misskey and Catodon build their HTTP client's CacheableLookup with `lookup: false`, so they never
// fall back to the OS resolver and ignore /etc/hosts; only names the upstream DNS knows resolve.
// Use: NODE_OPTIONS="--require /path/to/node-hosts-preload.cjs". Not for production.
const fs = require('fs');
const { promises } = require('dns');

function hostsV4(name) {
  let text;
  try { text = fs.readFileSync('/etc/hosts', 'utf8'); } catch { return null; }
  for (const line of text.split('\n')) {
    const parts = line.replace(/#.*/, '').trim().split(/\s+/);
    if (parts.length > 1 && /^\d+\.\d+\.\d+\.\d+$/.test(parts[0]) && parts.slice(1).includes(name)) return parts[0];
  }
  return null;
}

const R = promises.Resolver.prototype;
const o4 = R.resolve4, o6 = R.resolve6;
R.resolve4 = function (name, opts) {
  const ip = hostsV4(name);
  if (!ip) return o4.call(this, name, opts);
  return Promise.resolve(opts && opts.ttl ? [{ address: ip, ttl: 3600 }] : [ip]);
};
R.resolve6 = function (name, opts) {
  if (!hostsV4(name)) return o6.call(this, name, opts);
  const e = new Error(`queryAaaa ENODATA ${name}`); e.code = 'ENODATA'; e.syscall = 'queryAaaa'; e.hostname = name;
  return Promise.reject(e);
};
