/// Cowboy HTTP handler that serves the timeflies HTML page.
module FableActorTimefliesHttp

open Fable.Beam.Maps
open Fable.Beam.Cowboy.CowboyReq
open Fable.Beam.Cowboy.CowboyHandler

let private html =
    "<!DOCTYPE html>
<html>
<head>
  <title>Timeflies - Fable.Actor Demo</title>
  <style>
    body {
      margin: 0;
      padding: 0;
      background: #1a1a2e;
      overflow: hidden;
      font-family: 'Courier New', monospace;
      cursor: crosshair;
    }
    .letter {
      position: absolute;
      font-size: 24px;
      font-weight: bold;
      color: #eee;
      text-shadow: 0 0 10px #0ff, 0 0 20px #0ff;
      pointer-events: none;
    }
    #info {
      position: fixed;
      bottom: 20px;
      left: 20px;
      color: #666;
      font-size: 14px;
    }
    #title {
      position: fixed;
      top: 20px;
      left: 20px;
      color: #0ff;
      font-size: 18px;
    }
    #stats {
      position: fixed;
      top: 20px;
      right: 20px;
      color: #0ff;
      font-size: 14px;
      text-align: right;
      font-family: monospace;
    }
    .stat-value {
      color: #fff;
      font-weight: bold;
    }
  </style>
</head>
<body>
  <div id=\"title\">Fable.Actor Timeflies Demo</div>
  <div id=\"stats\">
    <div>In: <span id=\"in-rate\" class=\"stat-value\">0</span> msg/s</div>
    <div>Out: <span id=\"out-rate\" class=\"stat-value\">0</span> msg/s</div>
  </div>
  <div id=\"info\">Move your mouse...</div>
  <div id=\"letters\"></div>

  <script>
    const container = document.getElementById('letters');
    const letters = {};

    let inCount = 0;
    let outCount = 0;
    const inRateEl = document.getElementById('in-rate');
    const outRateEl = document.getElementById('out-rate');

    setInterval(() => {
      inRateEl.textContent = inCount;
      outRateEl.textContent = outCount;
      inCount = 0;
      outCount = 0;
    }, 1000);

    const ws = new WebSocket('ws://' + window.location.host + '/ws');

    ws.onopen = () => {
      document.getElementById('info').textContent = 'Connected! Move your mouse...';
    };

    ws.onmessage = (event) => {
      inCount++;
      const data = JSON.parse(event.data);
      if (data.index === undefined) return;

      let span = letters[data.index];
      if (!span) {
        span = document.createElement('span');
        span.className = 'letter';
        container.appendChild(span);
        letters[data.index] = span;
      }
      span.textContent = data.char;
      span.style.left = data.x + 'px';
      span.style.top = data.y + 'px';
    };

    ws.onclose = () => {
      document.getElementById('info').textContent = 'Disconnected. Refresh to reconnect.';
    };

    let lastSend = 0;
    document.addEventListener('mousemove', (e) => {
      const now = Date.now();
      if (now - lastSend > 16 && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ x: e.clientX, y: e.clientY }));
        outCount++;
        lastSend = now;
      }
    });
  </script>
</body>
</html>"

/// Cowboy content-type header map
let private htmlHeaders: BeamMap<string, string> =
    ofList [ ("content-type", "text/html") ]

let init (req: Req) (state: 'State) : HandlerResult<'State> =
    let req = reply 200 htmlHeaders html req
    ok req state
