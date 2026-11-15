#!/usr/bin/env node
// Execute the saved dashboard's actual JavaScript with explicit minimal DOM/fetch doubles.
// This verifies render data and error recovery. It is not a browser or layout test.
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const vm = require('node:vm');
const assert = require('node:assert/strict');

const repoRoot = process.cwd();
const htmlPath = path.resolve(process.argv[2] || 'state/restore-20261115-01/evidence/dashboard-response.html');
const snapshotPath = path.resolve(process.argv[3] || 'state/restore-20261115-01/evidence/dashboard-snapshot.json');
const reportPath = path.resolve(process.argv[4] || 'state/browser-20261115/dashboard-js-check.json');
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
const htmlBytes = fs.readFileSync(htmlPath);
const snapshotBytes = fs.readFileSync(snapshotPath);
const html = htmlBytes.toString('utf8');
const snapshot = JSON.parse(snapshotBytes.toString('utf8'));
const scripts = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)];
assert.equal(scripts.length, 1, 'expected exactly one inline dashboard script');
const script = scripts[0][1];

class ElementDouble {
  constructor(tag, id = null) {
    this.tagName = tag.toUpperCase();
    this.id = id;
    this.className = '';
    this.children = [];
    this.ownText = '';
  }
  set textContent(value) {
    this.ownText = String(value);
    this.children = [];
  }
  get textContent() {
    return this.ownText + this.children.map(child => typeof child === 'string' ? child : child.textContent).join('');
  }
  appendChild(child) { this.children.push(child); return child; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.ownText = ''; this.children = [...children]; }
}

const elements = new Map([...html.matchAll(/\bid="([^"]+)"/g)].map(match => [match[1], new ElementDouble('div', match[1])]));
const fetchCalls = [];
const intervals = [];
const unhandled = [];
const browserConsoleErrors = [];
let nextResponse = 'success';
const unhandledHandler = error => unhandled.push({name: error.name, message: error.message});
process.on('unhandledRejection', unhandledHandler);
const context = vm.createContext({
  document: {
    getElementById(id) { return elements.get(id) || null; },
    createElement(tag) { return new ElementDouble(tag); },
  },
  async fetch(url, options) {
    const mode = nextResponse;
    nextResponse = 'success';
    fetchCalls.push({url, options, response: mode === 'success' ? 200 : 503});
    if (mode === 'http503') return {ok: false, status: 503};
    assert.equal(mode, 'success', 'unexpected test response mode');
    return {ok: true, status: 200, async json() { return JSON.parse(snapshotBytes.toString('utf8')); }};
  },
  setInterval(callback, milliseconds) { intervals.push({callback, milliseconds}); return intervals.length; },
  console: {log() {}, error(...items) { browserConsoleErrors.push(items.map(String).join(' ')); }},
});

const element = id => {
  const found = elements.get(id);
  assert.ok(found, `missing HTML element ${id}`);
  return found;
};
const renderSummary = () => ({
  cards: element('cards').children.map(card => ({label: card.children[0].textContent, value: card.children[1].textContent})),
  alert_rows: element('alerts').children.length,
  prediction_rows: element('predictions').children.length,
  status_text: element('status').textContent,
  status_class: element('status').className,
  model_text: element('model').textContent,
  health: JSON.parse(element('health').textContent),
});
const report = {
  schema_version: '1',
  started_at: new Date().toISOString(),
  scope: 'actual inline dashboard JavaScript under explicit minimal DOM/fetch doubles using saved real HTTP responses',
  exclusions: ['browser engine', 'CSS/layout', 'accessibility', 'real network failure', 'Databricks execution'],
  runtime: {node: process.version, platform: process.platform, architecture: process.arch},
  inputs: {
    html: {path: path.relative(repoRoot, htmlPath), bytes: htmlBytes.length, sha256: sha256(htmlBytes)},
    inline_script: {bytes: Buffer.byteLength(script), sha256: sha256(script)},
    snapshot: {path: path.relative(repoRoot, snapshotPath), bytes: snapshotBytes.length, sha256: sha256(snapshotBytes)},
    checker: {path: path.relative(repoRoot, __filename), sha256: sha256(fs.readFileSync(__filename))},
  },
  snapshot_counts: snapshot.counts,
  checks: {},
  passed: false,
};

function check(name, action) {
  action();
  report.checks[name] = true;
}

(async () => {
  try {
    vm.runInContext(script, context, {filename: 'saved-dashboard-inline-script.js', timeout: 1000});
    await new Promise(resolve => setImmediate(resolve));
    const initial = renderSummary();
    report.initial_render = initial;
    check('five_metric_cards_match_snapshot', () => assert.deepEqual(initial.cards, [
      {label: 'Raw messages', value: String(snapshot.counts.raw)},
      {label: 'Accepted sensors', value: String(snapshot.counts.accepted)},
      {label: 'Rejected / duplicates', value: `${snapshot.counts.rejected} / ${snapshot.counts.duplicate}`},
      {label: 'Predictions', value: String(snapshot.counts.predictions)},
      {label: 'Maintenance alerts', value: String(snapshot.counts.alerts)},
    ]));
    check('alert_rows_match_documented_twenty_row_limit', () => assert.equal(initial.alert_rows, snapshot.alerts.length ? Math.min(20, snapshot.alerts.length) : 1));
    check('prediction_rows_match_documented_forty_row_limit', () => assert.equal(initial.prediction_rows, Math.min(40, snapshot.predictions.length)));
    check('health_uses_original_counts_rejections_runtime', () => assert.deepEqual(initial.health, {counts: snapshot.counts, rejections: snapshot.rejections, runtime: snapshot.runtime}));
    check('active_model_version_and_pending_count_rendered', () => {
      assert.ok(snapshot.deployment, 'this proof requires an active deployment in the original snapshot');
      assert.ok(initial.model_text.includes(`v${snapshot.deployment.model_version}`));
      assert.ok(initial.model_text.includes(`${snapshot.counts.pending_predictions} pending predictions`));
    });
    check('initial_refresh_succeeds_without_error_styling', () => {
      assert.equal(initial.status_class, '');
      assert.ok(initial.status_text.startsWith(`${snapshot.runtime.model.value.status} · `));
    });
    check('periodic_refresh_registered_at_five_seconds', () => {
      assert.equal(intervals.length, 1);
      assert.equal(intervals[0].milliseconds, 5000);
    });

    nextResponse = 'http503';
    await intervals[0].callback();
    const failed = renderSummary();
    report.forced_http503 = {status_text: failed.status_text, status_class: failed.status_class, response_status: 503};
    check('forced_http_failure_sets_visible_error', () => {
      assert.equal(failed.status_text, 'Pipeline unavailable');
      assert.equal(failed.status_class, 'error');
    });
    check('failed_refresh_preserves_last_rendered_data', () => {
      assert.deepEqual(failed.cards, initial.cards);
      assert.equal(failed.alert_rows, initial.alert_rows);
      assert.equal(failed.prediction_rows, initial.prediction_rows);
    });

    await intervals[0].callback();
    const recovered = renderSummary();
    report.recovered_render = {status_text: recovered.status_text, status_class: recovered.status_class, alert_rows: recovered.alert_rows, prediction_rows: recovered.prediction_rows};
    check('successful_refresh_clears_stale_error_class', () => assert.equal(recovered.status_class, ''));
    check('successful_refresh_restores_original_render_without_duplicate_rows', () => assert.deepEqual(recovered, initial));
    check('all_fetches_use_snapshot_endpoint_and_no_store', () => {
      assert.equal(fetchCalls.length, 3);
      for (const call of fetchCalls) {
        assert.equal(call.url, '/api/snapshot');
        assert.equal(call.options.cache, 'no-store');
      }
      assert.deepEqual(fetchCalls.map(call => call.response), [200, 503, 200]);
    });
    await new Promise(resolve => setImmediate(resolve));
    check('no_unhandled_js_rejections_or_console_errors', () => {
      assert.deepEqual(unhandled, []);
      assert.deepEqual(browserConsoleErrors, []);
    });
    report.passed = true;
  } catch (error) {
    report.failure = {name: error.name, message: error.message, stack: error.stack};
    process.exitCode = 1;
  } finally {
    report.fetch_calls = fetchCalls;
    report.unhandled_js_rejections = unhandled;
    report.console_errors = browserConsoleErrors;
    report.finished_at = new Date().toISOString();
    process.removeListener('unhandledRejection', unhandledHandler);
    fs.mkdirSync(path.dirname(reportPath), {recursive: true});
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2) + '\n');
    process.stdout.write(JSON.stringify(report, null, 2) + '\n');
  }
})();
