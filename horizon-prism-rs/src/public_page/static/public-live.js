// The public page's live feed (public-page design §6.6): every relay event,
// as it arrives. Rows are built with createElement and textContent only, at
// most once per animation frame, and the list keeps the newest 20.
(function () {
  'use strict';
  var off = document.getElementById('live-off');
  if (off) { off.hidden = true; }
  var list = document.getElementById('live-list');
  if (!list || !window.EventSource) { return; }
  var KEEP = 20;
  var notes = document.getElementById('live-notes');
  var status = document.getElementById('live-status');
  var received = document.getElementById('live-received');
  var rateEl = document.getElementById('live-rate');
  var inputEl = document.getElementById('live-input');
  var arrivals = document.getElementById('live-arrivals');

  function newId() {
    var b = new Uint8Array(16);
    crypto.getRandomValues(b);
    var s = '';
    for (var i = 0; i < b.length; i++) { s += (b[i] < 16 ? '0' : '') + b[i].toString(16); }
    return s;
  }
  var page = newId();
  var es = null;
  var pending = [];
  var frame = 0;
  var total = 0, thisSecond = 0;
  var lastC = null, lastBoot = null, backlogE = -1, firstLines = true, lastSeenMs = null;
  var slowAt = 0, failingSince = 0, delay = 10000, retryTimer = 0;

  function two(n) { return (n < 10 ? '0' : '') + n; }
  function hms(secs) {
    var d = new Date(secs * 1000);
    return two(d.getUTCHours()) + ':' + two(d.getUTCMinutes()) + ':' + two(d.getUTCSeconds());
  }
  function note(text) {
    var p = document.createElement('p');
    p.className = 'muted';
    p.textContent = '… ' + text;
    notes.insertBefore(p, notes.firstChild);
    while (notes.childNodes.length > 5) { notes.removeChild(notes.lastChild); }
  }
  function say(text) {
    status.textContent = text;
    status.hidden = !text;
  }
  function decode(ev) {
    var out = [];
    for (var i = 0; i < ev.l.length; i++) {
      var l = ev.l[i];
      var did = l[3] === 'p' ? 'did:plc:' + l[4] : (l[3] === 'w' ? 'did:web:' + l[4] : l[4]);
      out.push({ t: ev.t0 + l[0], a: l[1], n: ev.n[l[2]], d: did });
    }
    return out;
  }
  function render() {
    frame = 0;
    var frag = document.createDocumentFragment();
    for (var i = pending.length - 1; i >= 0; i--) {
      var l = pending[i];
      var li = document.createElement('li');
      var cells = [hms(l.t), l.a, l.n, l.d];
      for (var j = 0; j < cells.length; j++) {
        var span = document.createElement('span');
        span.textContent = cells[j];
        li.appendChild(span);
      }
      frag.appendChild(li);
    }
    pending = [];
    list.insertBefore(frag, list.firstChild);
    while (list.childNodes.length > KEEP) { list.removeChild(list.lastChild); }
  }
  function queue(lines) {
    for (var i = 0; i < lines.length; i++) { pending.push(lines[i]); }
    if (pending.length > KEEP) { pending = pending.slice(pending.length - KEEP); }
    if (!frame) { frame = requestAnimationFrame(render); }
  }
  function skippedText(s) {
    var span = 'events from ' + hms(Math.floor(s.from / 1000)) + ' to ' + hms(Math.floor(s.to / 1000)) + ' UTC were skipped: ';
    return span + (s.why === 'reset' ? 'the event stream was reset' : 'the live reader fell behind');
  }
  function stop(text) {
    if (es) { es.close(); es = null; }
    say(text);
  }
  function reconnect(wait) {
    if (es) { es.close(); es = null; }
    clearTimeout(retryTimer);
    retryTimer = setTimeout(open, wait);
  }
  function backOff() {
    if (es) { es.close(); es = null; }
    if (!failingSince) { failingSince = Date.now(); }
    if (Date.now() - failingSince > 15 * 60 * 1000) {
      say('The live feed stopped. Reload the page.');
      return;
    }
    say('reconnecting…');
    // Jittered: 10 to 60 s at first, doubling to 5 minutes.
    var wait = delay + Math.random() * delay * 5;
    delay = Math.min(delay * 2, 5 * 60 * 1000);
    reconnect(Math.min(wait, 5 * 60 * 1000));
  }
  function on(type, f) {
    es.addEventListener(type, function (m) {
      var ev;
      try { ev = JSON.parse(m.data); } catch (e) { return; }
      f(ev);
    });
  }
  function open() {
    if (es) { es.close(); es = null; }
    if (document.hidden) { return; }
    firstLines = true;
    es = new EventSource('/live?page=' + page);
    es.onopen = function () { failingSince = 0; delay = 10000; say(''); };
    es.onerror = function () {
      // A network drop is first left to EventSource's own retry.
      if (es && es.readyState === EventSource.CLOSED) { backOff(); }
    };
    on('backlog', function (ev) {
      // Every backlog starts a stream, EventSource's own reconnects
      // included: the next lines event is checked for a hole.
      firstLines = true;
      pending = [];
      backlogE = ev.e;
      if (lastBoot !== null && ev.boot !== lastBoot) {
        note('events not received while Horizon-Prism restarted');
        lastC = null;
      }
      lastBoot = ev.boot;
      while (list.firstChild) { list.removeChild(list.firstChild); }
      // A span that ended after the last line this tab saw: shown on return.
      if (ev.skipped && lastSeenMs !== null && ev.skipped.to > lastSeenMs) { note(skippedText(ev.skipped)); }
      queue(decode(ev));
    });
    on('lines', function (ev) {
      if (ev.e <= backlogE) { return; }
      var n = ev.l.length;
      if (firstLines) {
        firstLines = false;
        if (lastC !== null) {
          var hole = ev.c - n - lastC;
          if (hole > 0) { note(hole + ' events not received while reconnecting'); }
        }
      }
      lastC = ev.c;
      if (n > 0) { lastSeenMs = (ev.t0 + ev.l[n - 1][0]) * 1000 + 999; }
      total += n;
      thisSecond += n;
      received.textContent = total.toLocaleString();
      queue(decode(ev));
    });
    on('skipped', function (ev) {
      if (ev.e <= backlogE) { return; }
      note(skippedText(ev));
      lastSeenMs = ev.to;
    });
    on('gap', function (ev) {
      if (ev.lines > 0) { note(ev.lines + ' events missed: your connection could not keep up with the full feed'); }
      lastC = ev.c;
    });
    on('counts', function (ev) {
      if (ev.e <= backlogE) { return; }
      var kinds = [];
      for (var k in ev.arrivals) { kinds.push([k, ev.arrivals[k]]); }
      kinds.sort(function (a, b) { return b[1] - a[1]; });
      var rows = arrivals.children;
      for (var i = 0; i < 3; i++) {
        var l = rows[i].firstChild, v = rows[i].lastChild;
        l.removeAttribute('title');
        if (i < 2 || kinds.length <= 3) {
          l.textContent = kinds[i] ? kinds[i][0] : '';
          v.textContent = kinds[i] ? kinds[i][1].toLocaleString() + ' a second' : '';
        } else {
          var rest = kinds.slice(2), sum = 0, tip = [];
          for (var j = 0; j < rest.length; j++) { sum += rest[j][1]; tip.push(rest[j][0] + ' ' + rest[j][1]); }
          l.textContent = '+' + rest.length + ' more';
          l.title = tip.join(', ');
          v.textContent = sum.toLocaleString() + ' a second';
        }
      }
      rows[3].lastChild.textContent = ev.events.now.toLocaleString() + ' a second';
      inputEl.textContent = ev.input === null ? 'unknown' : Math.round(ev.input).toLocaleString();
      rateEl.textContent = thisSecond.toLocaleString();
      thisSecond = 0;
    });
    on('end', function (ev) {
      if (es) { es.close(); es = null; }
      var r = ev.reason;
      if (r === 'renew') { reconnect(0); }
      else if (r === 'replaced') { page = newId(); reconnect(0); }
      else if (r === 'rotated' || r === 'busy') { stop('The live feed is busy right now. The rest of this page is current.'); }
      else if (r === 'slow') {
        if (slowAt && Date.now() - slowAt < 10 * 60 * 1000) {
          stop("Your connection can't keep up with the full feed. The rest of this page is current.");
        } else {
          slowAt = Date.now();
          say('reconnecting…');
          reconnect(30000);
        }
      } else { backOff(); }
    });
  }
  document.addEventListener('visibilitychange', function () {
    if (document.hidden) {
      clearTimeout(retryTimer);
      if (es) { es.close(); es = null; }
    } else if (!es) {
      // Back in view: a new stream with the same page id, after a stop too.
      say('');
      open();
    }
  });
  open();
})();
