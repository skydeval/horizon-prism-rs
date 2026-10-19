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

  var MAX = 200;
  var statusEl = document.getElementById('live-status');
  var message = document.getElementById('live-message');
  var rates = document.getElementById('live-rates');
  var controls = document.getElementById('live-controls');
  var list = document.getElementById('live-list');
  var pauseBtn = document.getElementById('live-pause');
  var kindSel = document.getElementById('live-kind');
  var sourceSel = document.getElementById('live-source');
  var tiles = document.getElementById('tiles');
  if (!statusEl || !message || !rates || !controls || !list || !pauseBtn || !kindSel || !sourceSel) return;

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
  var held = []; // lines not yet on screen, newest first, at most MAX

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

  function lineEl(l) {
    var li = el('li');
    li.appendChild(el('span', 't', l.t));
    // A commit is applied once, from the relay that delivers it first;
    // later copies count as unchanged.
    var src = el('span', 'src', l.src === 'network' && l.relay ? 'network · first via ' + l.relay : l.src);
    if (l.src === 'network' && l.relay) src.title = 'Applied from ' + l.relay + ', the first relay to deliver it; later copies count as unchanged.';
    li.appendChild(src);
    li.appendChild(el('span', 'act', l.act));
    li.appendChild(el('span', 'k', KIND[l.k] || l.k));
    var who = el('span', 'who', l.who || l.did);
    who.title = l.did;
    li.appendChild(who);
    if (typeof l.text === 'string') li.appendChild(el('span', 'text', '“' + l.text + '”'));
    return li;
  }

  function render() {
    if (!held.length) return;
    var frag = document.createDocumentFragment();
    for (var i = 0; i < held.length; i++) frag.appendChild(lineEl(held[i]));
    list.insertBefore(frag, list.firstChild);
    while (list.childNodes.length > MAX) list.removeChild(list.lastChild);
    held = [];
  }

  function onLines(e) {
    var lines;
    try { lines = JSON.parse(e.data); } catch (x) { return; }
    // A batch is newest last; held is newest first.
    for (var i = 0; i < lines.length; i++) held.unshift(lines[i]);
    if (held.length > MAX) held.length = MAX;
    if (paused) {
      pauseBtn.textContent = 'Resume (' + held.length + ' new lines)';
      return;
    }
    if (document.visibilityState === 'hidden') return;
    render();
  }

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
    var filtered = kindSel.value || sourceSel.value;
    var offered = c.shown + c.not_shown;
    var of = filtered ? offered : Math.max(offered, c.indexed);
    if (of > c.shown) indexed.appendChild(el('p', 'muted', 'Showing about ' + fmt(c.shown / secs) + ' of ' + fmt(of / secs) + ' per second.'));
    box.appendChild(indexed);
    var skipped = el('div');
    skipped.appendChild(el('h3', null, 'Skipped and rejected'));
    var reasons = Object.keys(c.reasons || {});
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
    held = [];
    list.replaceChildren();
    connect();
  }

  pauseBtn.addEventListener('click', function () {
    paused = !paused;
    if (paused) {
      pauseBtn.textContent = 'Resume';
      if (!stopped) setStatus('paused');
    } else {
      pauseBtn.textContent = 'Pause';
      if (!stopped) setStatus('live', 'on');
      render();
    }
  });
  kindSel.addEventListener('change', refilter);
  sourceSel.addEventListener('change', refilter);
  document.addEventListener('visibilitychange', function () {
    if (document.visibilityState === 'visible' && !paused) render();
  });

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
  list.hidden = false;
  setStatus('connecting…');
  connect();
  setInterval(refreshTiles, 30000);
})();
