// Home's live feed (admin-ui-live-feed design §5.3). Loaded by Home only,
// from this origin. Every handle and snippet is someone else's text, so
// lines are built with createElement and textContent, never parsed as
// markup.
// No style attribute is set anywhere: classes and the hidden attribute.
(function () {
  'use strict';
  // First act: the page is running its script, so the notice that says it
  // did not start goes (§5.4).
  var notice = document.getElementById('live-notstarted');
  if (notice) notice.hidden = true;

  // The two tickers (Changed by skydeval 2026-10-19): "On the firehose"
  // and "Indexed here", each a fixed-height box that shows one new line
  // every PACE_MS at the top and drops lines off the bottom.
  var PACE_MS = 200;    // one line per ticker this often: the one knob for pace
  var QUEUE_MAX = 20;   // lines waiting per ticker; older ones are dropped
  var ROWS_KEPT = 16;   // lines kept per ticker: the 14 shown and two below
  var SLOTS = 3;        // rows per column of figures, whatever is counted
  var statusEl = document.getElementById('live-status');
  var message = document.getElementById('live-message');
  var rates = document.getElementById('live-rates');
  var controls = document.getElementById('live-controls');
  var tickers = document.getElementById('live-tickers');
  var arrivingList = document.getElementById('live-arriving');
  var indexedList = document.getElementById('live-indexed');
  var pauseBtn = document.getElementById('live-pause');
  var kindSel = document.getElementById('live-kind');
  var sourceSel = document.getElementById('live-source');
  var tiles = document.getElementById('tiles');
  var helpBtn = document.getElementById('live-help-btn');
  var help = document.getElementById('live-help');
  if (!statusEl || !message || !rates || !controls || !tickers || !arrivingList || !indexedList || !pauseBtn || !kindSel || !sourceSel) return;

  var KIND = {
    'list-item': 'list item', 'list-block': 'list block', 'list-optout': 'list opt-out',
    'starter-pack': 'starter pack', 'private-post': 'private post', 'private-block': 'private block',
    'private-post-gate': 'private-post gate'
  };
  var REASON = {
    unindexed: 'not indexed', kryphocron_unproduced: 'private type not produced', not_in_scope: 'not in scope',
    public_off: 'public data off', account_deleted: 'account deleted', out_of_scope: 'leaving scope',
    member_removed: 'member removed', other: 'other'
  };
  var ENDED = {
    signed_out: 'you signed out', expired: 'your session ended', not_operator: 'you are no longer an operator',
    host_not_allowed: "your account's server is no longer allowed", pds_changed: 'your account moved to another server',
    could_not_confirm: 'Prism could not confirm your account', replaced: 'the feed was opened in another tab'
  };

  // One page id per page load: a reconnect replaces this page's own stream.
  var bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  var page = Array.prototype.map.call(bytes, function (b) { return ('0' + b.toString(16)).slice(-2); }).join('');

  var es = null;
  var paused = false;
  var stopped = false;

  function setStatus(text, cls) {
    statusEl.textContent = text;
    statusEl.className = 'pill' + (cls ? ' ' + cls : '');
    statusEl.hidden = false;
  }

  function stop(text) {
    stopped = true;
    if (es) es.close();
    setStatus('stopped', 'off');
    message.textContent = text;
    message.hidden = false;
  }

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  }

  function fmt(x) {
    return x >= 10 ? Math.round(x).toLocaleString() : x.toFixed(1);
  }

  // An op's fate in words, for the firehose ticker: indexed, a skip or
  // reject reason, or a duplicate (a commit already applied from the relay
  // that delivered it first).
  function outcome(l) {
    if (!l.o) return 'indexed';
    if (l.o === 'unchanged') return l.src === 'network' ? 'duplicate' : 'unchanged';
    if (l.o === 'failed') return 'failed';
    return REASON[l.o] || l.o;
  }

  // One line, on one row. withText: the indexed ticker, the only one that
  // shows a public post's snippet; a line that was not indexed has none.
  // slide: a paced live line slides in; a backlog line just appears.
  function lineEl(l, withText, slide) {
    var li = el('li', (slide ? 'enter' : '') + (l.o ? ' dim' : ''));
    li.appendChild(el('span', 't', l.t));
    if (!withText) li.appendChild(el('span', l.o ? 'o' : 'o ix', outcome(l)));
    var src;
    if (l.src === 'network' && l.relay) {
      // A commit is applied once, from the relay that delivers it first;
      // later copies count as unchanged.
      src = el('span', 'src', (l.o ? 'via ' : 'first via ') + l.relay);
      src.title = l.o ? 'From the network, via ' + l.relay + '.'
        : 'From the network: applied from ' + l.relay + ', the first relay to deliver it; later copies count as unchanged.';
    } else {
      src = el('span', 'src', l.src);
    }
    li.appendChild(src);
    li.appendChild(el('span', 'act', l.act));
    li.appendChild(el('span', 'k', KIND[l.k] || l.k));
    var who = el('span', 'who', l.who || l.did);
    who.title = l.did;
    li.appendChild(who);
    if (withText && !l.o && typeof l.text === 'string') li.appendChild(el('span', 'text', '“' + l.text + '”'));
    return li;
  }

  // A ticker: lines wait in a short queue (newest kept, oldest dropped)
  // and enter at the top one at a time; past ROWS_KEPT they leave the page.
  // Every line has a number (n), rising; a ticker takes each number once,
  // so a reconnect's backlog never shows a line twice.
  function ticker(list, withText) {
    var queue = [];
    var last = 0;
    function take(l) {
      if (typeof l.n !== 'number') return true;
      if (l.n <= last) return false;
      last = l.n;
      return true;
    }
    function push(l) {
      if (!take(l)) return;
      queue.push(l);
      if (queue.length > QUEUE_MAX) queue.splice(0, queue.length - QUEUE_MAX);
    }
    function trim() {
      while (list.childNodes.length > ROWS_KEPT) list.removeChild(list.lastChild);
    }
    return {
      push: push,
      step: function () {
        if (!queue.length) return;
        list.insertBefore(lineEl(queue.shift(), withText, true), list.firstChild);
        trim();
      },
      // The backlog a stream starts with (oldest first): the lines this
      // page has not had yet, shown at once, without the pace or the
      // slide, newest on top. Paused or hidden, they wait like any line.
      backlog: function (lines) {
        lines.forEach(push);
        if (paused || document.visibilityState === 'hidden') return;
        var frag = document.createDocumentFragment();
        while (queue.length) frag.insertBefore(lineEl(queue.shift(), withText, false), frag.firstChild);
        list.insertBefore(frag, list.firstChild);
        trim();
      },
      clear: function () {
        queue.length = 0;
        last = 0;
        list.replaceChildren();
      }
    };
  }
  var firehoseTicker = ticker(arrivingList, false);
  var indexedTicker = ticker(indexedList, true);

  function onLines(e) {
    var lines;
    try { lines = JSON.parse(e.data); } catch (x) { return; }
    // A batch is newest last.
    for (var i = 0; i < lines.length; i++) {
      if (lines[i].f) firehoseTicker.push(lines[i]);
      if (lines[i].i) indexedTicker.push(lines[i]);
    }
  }

  // Sent once when a stream opens, before any live line.
  function onBacklog(e) {
    var lines;
    try { lines = JSON.parse(e.data); } catch (x) { return; }
    firehoseTicker.backlog(lines.filter(function (l) { return l.f; }));
    indexedTicker.backlog(lines.filter(function (l) { return l.i; }));
  }

  // The pace: paused or hidden, the tickers stand still and only their
  // short queues refresh.
  setInterval(function () {
    if (paused || document.visibilityState === 'hidden') return;
    firehoseTicker.step();
    indexedTicker.step();
  }, PACE_MS);

  // The figures: built once, then only their text changes, so nothing
  // moves. Each column has SLOTS rows: blank rows fill a short column, and
  // a long one's last row reads "+N more", the rest named in its tooltip.
  var stats = null;
  function statsBox() {
    if (stats) return stats;
    var since = el('p', 'muted live-since');
    var box = el('div', 'grid');
    function column(title) {
      var d = el('div');
      var h = el('h3', null, title);
      d.appendChild(h);
      var rows = [];
      for (var i = 0; i < SLOTS; i++) {
        var row = el('div', 'fact blank');
        var l = el('span', 'l', ' ');
        var v = el('span', 'v', ' ');
        row.appendChild(l);
        row.appendChild(v);
        d.appendChild(row);
        rows.push({ row: row, l: l, v: v });
      }
      box.appendChild(d);
      return { h: h, rows: rows };
    }
    stats = { since: since, indexed: column('Indexed'), skipped: column('Skipped and rejected'), also: column('Also') };
    rates.replaceChildren(since, box);
    return stats;
  }

  function setRow(r, label, value, tip) {
    r.row.className = label === null ? 'fact blank' : 'fact';
    r.l.textContent = label === null ? ' ' : label;
    r.v.textContent = value === null ? ' ' : value;
    if (tip) r.row.title = tip; else r.row.removeAttribute('title');
  }

  // items: [label, count], largest first.
  function fillColumn(col, items, per, none) {
    var shown = items;
    if (items.length > SLOTS) {
      var rest = items.slice(SLOTS - 1);
      var sum = rest.reduce(function (n, it) { return n + it[1]; }, 0);
      var tip = rest.map(function (it) { return it[0] + ': ' + per(it[1]); }).join(', ');
      shown = items.slice(0, SLOTS - 1).concat([['+' + rest.length + ' more', sum, tip]]);
    }
    for (var i = 0; i < SLOTS; i++) {
      var it = shown[i];
      if (it) setRow(col.rows[i], it[0], per(it[1]), it[2]);
      else if (i === 0 && none) setRow(col.rows[i], none, null);
      else setRow(col.rows[i], null, null);
    }
  }

  function largestFirst(map, words) {
    return Object.keys(map || {})
      .map(function (k) { return [words[k] || k, map[k]]; })
      .sort(function (a, b) { return b[1] - a[1]; });
  }

  function onCounts(e) {
    var c;
    try { c = JSON.parse(e.data); } catch (x) { return; }
    var secs = c.secs > 0 ? c.secs : 1;
    var per = function (n) { return fmt(n / secs) + '/s'; };
    var s = statsBox();
    s.indexed.h.textContent = 'Indexed: ' + per(c.indexed);
    fillColumn(s.indexed, largestFirst(c.kinds, KIND), per, 'none this second');
    fillColumn(s.skipped, largestFirst(c.reasons, REASON), per, 'none this second');
    fillColumn(s.also, [['unchanged', c.unchanged], ['failed', c.failed], ['identity and account events', c.events]], per, null);
    var t = c.totals || {};
    var text = 'Since ' + t.since + ': ' + (t.indexed || 0).toLocaleString() + ' indexed · ' +
      (t.skipped || 0).toLocaleString() + ' skipped or rejected · ' + (t.failed || 0).toLocaleString() + ' failed';
    s.since.textContent = text;
    s.since.title = text;
  }

  // The "?" beside the heading: a short help that opens over the card.
  // Escape or a click elsewhere closes it.
  function setHelp(open) {
    help.hidden = !open;
    helpBtn.setAttribute('aria-expanded', open ? 'true' : 'false');
  }
  if (helpBtn && help) {
    helpBtn.hidden = false;
    helpBtn.addEventListener('click', function () { setHelp(help.hidden); });
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && !help.hidden) {
        setHelp(false);
        helpBtn.focus();
      }
    });
    document.addEventListener('click', function (e) {
      if (!help.hidden && e.target !== helpBtn && !help.contains(e.target)) setHelp(false);
    });
  }

  function onEnd(e) {
    var reason = '';
    try { reason = JSON.parse(e.data).reason; } catch (x) { /* none */ }
    if (es) es.close();
    if (reason === 'renew') {
      connect();
      return;
    }
    if (reason === 'shutting_down') {
      stop('Prism is restarting. This page reloads in a moment.');
      setTimeout(function () { location.reload(); }, 10000);
      return;
    }
    stop('The live feed stopped: ' + (ENDED[reason] || reason) + '. Reload the page.');
  }

  function url() {
    var q = '/admin/live?page=' + page;
    if (kindSel.value) q += '&kinds=' + encodeURIComponent(kindSel.value);
    if (sourceSel.value) q += '&sources=' + encodeURIComponent(sourceSel.value);
    return q;
  }

  function connect() {
    if (stopped) return;
    if (es) es.close();
    es = new EventSource(url());
    es.addEventListener('open', function () {
      if (!stopped) setStatus(paused ? 'paused' : 'live', paused ? null : 'on');
    });
    es.addEventListener('backlog', onBacklog);
    es.addEventListener('lines', onLines);
    es.addEventListener('counts', onCounts);
    es.addEventListener('end', onEnd);
    es.addEventListener('error', function () {
      if (stopped) return;
      // A 401, 429 or 503 at open is final: the browser does not retry.
      if (es.readyState === EventSource.CLOSED) stop('The live feed stopped. Reload the page.');
      else setStatus('reconnecting…');
    });
  }

  function refilter() {
    firehoseTicker.clear();
    indexedTicker.clear();
    connect();
  }

  // Pause freezes both tickers; their queues keep only the newest lines.
  pauseBtn.addEventListener('click', function () {
    paused = !paused;
    if (paused) {
      pauseBtn.textContent = 'Resume';
      if (!stopped) setStatus('paused');
    } else {
      pauseBtn.textContent = 'Pause';
      if (!stopped) setStatus('live', 'on');
    }
  });
  kindSel.addEventListener('change', refilter);
  sourceSel.addEventListener('change', refilter);

  // The tiles and the attention list, every 30 seconds, without reloading
  // the page (§5.4).
  function refreshTiles() {
    if (stopped || !tiles) return;
    fetch('/admin/tiles', { credentials: 'same-origin', cache: 'no-store' }).then(function (res) {
      if (res.redirected || res.status !== 200) {
        stop('Your session ended. Reload the page.');
        return null;
      }
      return res.text();
    }).then(function (text) {
      if (text === null || text === undefined) return;
      var doc = new DOMParser().parseFromString(text, 'text/html');
      ['attention', 'tiles'].forEach(function (id) {
        var here = document.getElementById(id);
        var fresh = doc.getElementById(id);
        if (!here || !fresh) return;
        var nodes = [];
        fresh.childNodes.forEach(function (n) { nodes.push(document.importNode(n, true)); });
        here.replaceChildren.apply(here, nodes);
      });
    }).catch(function () { /* the next refresh tries again */ });
  }

  controls.hidden = false;
  tickers.hidden = false;
  setStatus('connecting…');
  connect();
  setInterval(refreshTiles, 30000);
})();
