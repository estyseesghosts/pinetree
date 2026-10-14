// Loopback reverse proxy for the Phase 2 interop harness.
//
// Misskey's WebFinger URL builder drops the port from URL queries
// (`u.hostname`), so every node must be reachable on port 80 of its own
// loopback IP. Usage: node proxy.mjs 127.0.1.1=3101 127.0.1.2=3102 ...
// Each `ip=port` pair listens on `ip:80` and forwards raw requests (Host and
// body untouched, so HTTP signatures and digests stay valid) to 127.0.0.1:port.
import http from 'node:http';

for (const pair of process.argv.slice(2)) {
  const [ip, port] = pair.split('=');
  const server = http.createServer((req, res) => {
    const upstream = http.request(
      { host: '127.0.0.1', port: Number(port), method: req.method, path: req.url, headers: req.headers },
      (up) => {
        res.writeHead(up.statusCode ?? 502, up.headers);
        up.pipe(res);
      },
    );
    upstream.on('error', () => {
      res.writeHead(502);
      res.end();
    });
    req.pipe(upstream);
  });
  server.on('upgrade', (req, socket, head) => {
    const s = net.connect(Number(port), '127.0.0.1', () => {
      s.write(`${req.method} ${req.url} HTTP/1.1\r\n` + Object.entries(req.headers).map(([k, v]) => `${k}: ${v}`).join('\r\n') + '\r\n\r\n');
      s.write(head);
      socket.pipe(s).pipe(socket);
    });
    s.on('error', () => socket.destroy());
  });
  server.listen(80, ip, () => console.log(`proxy ${ip}:80 -> 127.0.0.1:${port}`));
}
import net from 'node:net';
