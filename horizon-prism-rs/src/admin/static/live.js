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

  function fact(label, value) {
    var d = el('div', 'fact');
    d.appendChild(el('span', 'l', label));
    d.appendChild(el('span', 'v', value));
    return d;
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
  function lineEl(l, withText) {
    var li = el('li', l.o ? 'enter dim' : 'enter');
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
  function ticker(list, withText) {
    var queue = [];
    return {
      push: function (l) {
        queue.push(l);
        if (queue.length > QUEUE_MAX) queue.splice(0, queue.length - QUEUE_MAX);
      },
      step: function () {
        if (!queue.length) return;
        list.insertBefore(lineEl(queue.shift(), withText), list.firstChild);
        while (list.childNodes.length > ROWS_KEPT) list.removeChild(list.lastChild);
      },
      clear: function () {
        queue.length = 0;
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

  // The pace: paused or hidden, the tickers stand still and only their
  // short queues refresh.
  setInterval(function () {
    if (paused || document.visibilityState === 'hidden') return;
    firehoseTicker.step();
    indexedTicker.step();
  }, PACE_MS);

  function onCounts(e) {
    var c;
    try { c = JSON.parse(e.data); } catch (x) { return; }
    var secs = c.secs > 0 ? c.secs : 1;
    var per = function (n) { return fmt(n / secs) + '/s'; };
    var box = el('div', 'grid');
    var indexed = el('div');
    indexed.appendChild(el('h3', null, 'Indexed: ' + per(c.indexed)));
    var kinds = Object.keys(c.kinds || {}).sort(function (a, b) { return c.kinds[b] - c.kinds[a]; });
    var rest = 0;
    kinds.forEach(function (k, i) {
      if (i < 6) indexed.appendChild(fact(KIND[k] || k, per(c.kinds[k])));
      else rest += c.kinds[k];
    });
    if (rest > 0) indexed.appendChild(fact('other', per(rest)));
    var reasons = Object.keys(c.reasons || {});
    var skippedN = reasons.reduce(function (n, r) { return n + c.reasons[r]; }, 0);
    var arrivingN = (kindSel.value || sourceSel.value) ? c.shown + c.not_shown : c.indexed + skippedN + c.unchanged + c.failed;
    var pace = 1000 / PACE_MS;
    if (arrivingN / secs > pace) {
      indexed.appendChild(el('p', 'muted', 'The tickers show up to ' + fmt(pace) + ' lines a second each, a sample of about ' + fmt(arrivingN / secs) + ' arriving.'));
    }
    box.appendChild(indexed);
    var skipped = el('div');
    skipped.appendChild(el('h3', null, 'Skipped and rejected'));
    if (!reasons.length) skipped.appendChild(el('p', 'muted', 'None in the last second.'));
    reasons.forEach(function (r) { skipped.appendChild(fact(REASON[r] || r, per(c.reasons[r]))); });
    box.appendChild(skipped);
    var also = el('div');
    also.appendChild(el('h3', null, 'Also'));
    also.appendChild(fact('unchanged', per(c.unchanged)));
    also.appendChild(fact('failed', per(c.failed)));
    also.appendChild(fact('identity and account events', per(c.events)));
    box.appendChild(also);
    var t = c.totals || {};
    var since = el('p', 'muted', 'Since Prism started (' + t.since + '): ' + (t.indexed || 0).toLocaleString() +
      ' indexed, ' + (t.skipped || 0).toLocaleString() + ' skipped or rejected, ' + (t.failed || 0).toLocaleString() + ' failed.');
    rates.replaceChildren(since, box);
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

  // The tiles, every 30 seconds, without reloading the page (§5.4).
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
      var fresh = new DOMParser().parseFromString(text, 'text/html').getElementById('tiles');
      if (!fresh) return;
      var nodes = [];
      fresh.childNodes.forEach(function (n) { nodes.push(document.importNode(n, true)); });
      tiles.replaceChildren.apply(tiles, nodes);
    }).catch(function () { /* the next refresh tries again */ });
  }

  controls.hidden = false;
  tickers.hidden = false;
  setStatus('connecting…');
  connect();
  setInterval(refreshTiles, 30000);
})();
