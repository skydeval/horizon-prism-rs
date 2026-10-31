// The public page's live feed: every relay event, as it arrives. Rows are
// built with createElement and textContent only, at most once per animation
// frame, and the list keeps the newest 20. The counts are the server's,
// kept across restarts, each event counted once whichever relay sent it.
(function () {
  'use strict';
  var off = document.getElementById('live-off');
  if (off) { off.hidden = true; }
  var list = document.getElementById('live-list');
  if (!list || !window.EventSource) { return; }
  var KEEP = 20;
  var status = document.getElementById('live-status');
  var sinceEl = document.getElementById('live-since');
  var cells = {
    received: document.getElementById('live-received'),
    rate: document.getElementById('live-rate'),
    likes: document.getElementById('live-likes'),
    posts: document.getElementById('live-posts'),
    blocks: document.getElementById('live-blocks'),
    identity: document.getElementById('live-identity')
  };

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
  var backlogE = -1;
  var slowAt = 0, failingSince = 0, delay = 10000, retryTimer = 0;

  function two(n) { return (n < 10 ? '0' : '') + n; }
  function hms(secs) {
    var d = new Date(secs * 1000);
    return two(d.getUTCHours()) + ':' + two(d.getUTCMinutes()) + ':' + two(d.getUTCSeconds());
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
      var row = [hms(l.t), l.a, l.n, l.d];
      for (var j = 0; j < row.length; j++) {
        var span = document.createElement('span');
        span.textContent = row[j];
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
  // The sum of `keys` in a counts map; every key when `keys` is null.
  function sum(map, keys) {
    var n = 0;
    if (keys === null) {
      for (var k in map) { n += map[k]; }
    } else {
      for (var i = 0; i < keys.length; i++) { n += map[keys[i]] || 0; }
    }
    return n;
  }
  function both(total, now) {
    return total.toLocaleString() + ' · ' + now.toLocaleString() + ' a second';
  }
  function show(ev) {
    var t = ev.totals, a = ev.arrivals;
    cells.received.textContent = ev.received.total.toLocaleString();
    cells.rate.textContent = ev.received.now.toLocaleString();
    cells.likes.textContent = both(sum(t, ['like']), sum(a, ['like']));
    var posts = ['post', 'reply', 'repost'];
    cells.posts.textContent = both(sum(t, posts), sum(a, posts));
    var blocks = ['block', 'list-block'];
    cells.blocks.textContent = both(sum(t, blocks), sum(a, blocks));
    cells.identity.textContent = both(ev.events.total, ev.events.now);
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
    es = new EventSource('/live?page=' + page);
    es.onopen = function () { failingSince = 0; delay = 10000; say(''); };
    es.onerror = function () {
      // A network drop is first left to EventSource's own retry.
      if (es && es.readyState === EventSource.CLOSED) { backOff(); }
    };
    on('backlog', function (ev) {
      pending = [];
      backlogE = ev.e;
      if (ev.since) {
        sinceEl.textContent = ev.kinds_since && ev.kinds_since !== ev.since
          ? 'Events counted since ' + ev.since + ' UTC; the rest since ' + ev.kinds_since + ' UTC.'
          : 'Counted since ' + ev.since + ' UTC.';
        sinceEl.hidden = false;
      }
      while (list.firstChild) { list.removeChild(list.firstChild); }
      queue(decode(ev));
    });
    on('lines', function (ev) {
      if (ev.e <= backlogE) { return; }
      queue(decode(ev));
    });
    on('counts', function (ev) {
      if (ev.e <= backlogE) { return; }
      show(ev);
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
