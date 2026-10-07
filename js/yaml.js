/* yaml.js: a small YAML subset parser and dumper, enough for databricks.yml bundle files.
   Supports: maps, sequences, scalars, quotes, flow [] {}, block scalars | >, comments, --- docs. */
(function (K) {
  'use strict';

  class YAMLError extends Error {
    constructor(msg, line) { super(line ? 'line ' + line + ': ' + msg : msg); this.line = line; }
  }

  function findQuoteEnd(s, start) {
    const q = s[start];
    for (let i = start + 1; i < s.length; i++) {
      if (q === '"' && s[i] === '\\') { i++; continue; }
      if (s[i] === q) {
        if (q === "'" && s[i + 1] === "'") { i++; continue; }
        return i;
      }
    }
    return -1;
  }

  function unquote(t) {
    if (t[0] === "'") return t.slice(1, -1).replace(/''/g, "'");
    try { return JSON.parse(t); } catch (e) { return t.slice(1, -1); }
  }

  function stripComment(s) {
    let q = null;
    for (let i = 0; i < s.length; i++) {
      const ch = s[i];
      if (q) {
        if (q === '"' && ch === '\\') { i++; continue; }
        if (ch === q) q = null;
        continue;
      }
      if (ch === '"' || ch === "'") {
        const prev = s.slice(0, i).trimEnd();
        if (prev === '' || /[:\-[{,]$/.test(prev)) { q = ch; continue; }
      }
      if (ch === '#' && (i === 0 || /\s/.test(s[i - 1]))) return s.slice(0, i);
    }
    return s;
  }

  function splitKV(c) {
    if (c[0] === '"' || c[0] === "'") {
      const end = findQuoteEnd(c, 0);
      if (end < 0) return null;
      const m = /^\s*:(\s+|$)/.exec(c.slice(end + 1));
      if (!m) return null;
      return { key: unquote(c.slice(0, end + 1)), rest: c.slice(end + 1 + m[0].length).trim() };
    }
    if (c[0] === '[' || c[0] === '{') return null;
    for (let i = 0; i < c.length; i++) {
      if (c[i] === ':' && (i === c.length - 1 || c[i + 1] === ' ')) {
        const key = c.slice(0, i).trim();
        return key ? { key, rest: c.slice(i + 1).trim() } : null;
      }
    }
    return null;
  }

  const isSeq = c => c === '-' || c.startsWith('- ');

  function parseScalar(s, no) {
    s = s.trim();
    if (s === '' || s === '~' || /^null$/i.test(s)) return null;
    if (s[0] === '"' || s[0] === "'") {
      const end = findQuoteEnd(s, 0);
      if (end !== s.length - 1) throw new YAMLError('unexpected characters after quoted string', no);
      return unquote(s);
    }
    if (s[0] === '[' || s[0] === '{') return parseFlow(s, no);
    if (s[0] === '&' || s[0] === '*') throw new YAMLError('anchors and aliases are not supported here', no);
    if (/^(true|false)$/i.test(s)) return /^true$/i.test(s);
    if (/^[-+]?\d+$/.test(s)) return parseInt(s, 10);
    if (/^[-+]?(\d+\.\d*|\.\d+)([eE][-+]?\d+)?$/.test(s)) return parseFloat(s);
    return s;
  }

  function parseFlow(s, no) {
    let i = 0;
    const ws = () => { while (i < s.length && /\s/.test(s[i])) i++; };
    function tok(isKey) {
      ws();
      if (s[i] === '"' || s[i] === "'") {
        const end = findQuoteEnd(s, i);
        if (end < 0) throw new YAMLError('unterminated quoted string', no);
        const t = unquote(s.slice(i, end + 1)); i = end + 1; return { v: t, q: true };
      }
      const start = i;
      const stop = isKey ? /[:,\]}]/ : /[,\]}]/;
      while (i < s.length && !stop.test(s[i])) i++;
      return { v: s.slice(start, i).trim(), q: false };
    }
    function value() {
      ws();
      if (s[i] === '[') {
        i++; const arr = []; ws();
        if (s[i] === ']') { i++; return arr; }
        for (;;) {
          arr.push(value()); ws();
          if (s[i] === ',') { i++; continue; }
          if (s[i] === ']') { i++; return arr; }
          throw new YAMLError('expected "," or "]" in flow sequence', no);
        }
      }
      if (s[i] === '{') {
        i++; const obj = {}; ws();
        if (s[i] === '}') { i++; return obj; }
        for (;;) {
          const k = tok(true).v; ws();
          if (s[i] !== ':') throw new YAMLError('expected ":" in flow mapping', no);
          i++; obj[k] = value(); ws();
          if (s[i] === ',') { i++; continue; }
          if (s[i] === '}') { i++; return obj; }
          throw new YAMLError('expected "," or "}" in flow mapping', no);
        }
      }
      const t = tok(false);
      return t.q ? t.v : parseScalar(t.v, no);
    }
    const v = value(); ws();
    if (i < s.length) throw new YAMLError('unexpected characters after flow collection', no);
    return v;
  }

  class Parser {
    constructor(lines) { this.lines = lines; this.i = 0; }
    peek() {
      while (this.i < this.lines.length) {
        const L = this.lines[this.i];
        const c = stripComment(L.text);
        if (c.trim() === '') { this.i++; continue; }
        if (/^ *\t/.test(L.text)) throw new YAMLError('found a tab character where indentation is expected (use spaces)', L.no);
        const ind = /^ */.exec(c)[0].length;
        return { indent: ind, content: c.slice(ind).trimEnd(), no: L.no };
      }
      return null;
    }
    node(indent) {
      const L = this.peek();
      if (!L || L.indent < indent) return null;
      if (isSeq(L.content)) return this.seq(L.indent);
      if (splitKV(L.content)) return this.map(L.indent);
      this.i++;
      return parseScalar(L.content, L.no);
    }
    map(indent, first) {
      const obj = {};
      const handle = (content, no) => {
        const kv = splitKV(content);
        if (!kv) throw new YAMLError('expected "key: value"', no);
        if (Object.prototype.hasOwnProperty.call(obj, kv.key)) throw new YAMLError('duplicate key "' + kv.key + '"', no);
        if (kv.rest === '') {
          const N = this.peek();
          obj[kv.key] = N && (N.indent > indent || (N.indent === indent && isSeq(N.content))) ? this.node(N.indent) : null;
        } else if (/^[|>][+-]?$/.test(kv.rest)) {
          obj[kv.key] = this.block(indent, kv.rest);
        } else {
          obj[kv.key] = parseScalar(kv.rest, no);
        }
      };
      if (first) handle(first.content, first.no);
      for (;;) {
        const L = this.peek();
        if (!L || L.indent < indent) break;
        if (L.indent > indent) throw new YAMLError('bad indentation of a mapping entry', L.no);
        if (isSeq(L.content)) break;
        this.i++;
        handle(L.content, L.no);
      }
      return obj;
    }
    seq(indent) {
      const arr = [];
      for (;;) {
        const L = this.peek();
        if (!L || L.indent !== indent || !isSeq(L.content)) {
          if (L && L.indent > indent) throw new YAMLError('bad indentation of a sequence entry', L.no);
          break;
        }
        this.i++;
        const rest = L.content.slice(1).replace(/^ +/, '');
        const col = indent + (L.content.length - rest.length);
        if (rest === '') {
          const N = this.peek();
          arr.push(N && N.indent > indent ? this.node(N.indent) : null);
        } else if (isSeq(rest)) {
          throw new YAMLError('nested inline sequences are not supported', L.no);
        } else if (splitKV(rest)) {
          arr.push(this.map(col, { content: rest, no: L.no }));
        } else {
          arr.push(parseScalar(rest, L.no));
        }
      }
      return arr;
    }
    block(parentIndent, header) {
      const out = []; let bi = null;
      while (this.i < this.lines.length) {
        const raw = this.lines[this.i].text;
        if (raw.trim() === '') { out.push(''); this.i++; continue; }
        const ind = /^ */.exec(raw)[0].length;
        if (ind <= parentIndent || (bi !== null && ind < bi)) break;
        if (bi === null) bi = ind;
        out.push(raw.slice(bi)); this.i++;
      }
      if (!header.includes('+')) while (out.length && out[out.length - 1] === '') out.pop();
      let s = header[0] === '|' ? out.join('\n') : out.join(' ').replace(/ {2,}/g, ' ');
      if (!header.includes('-')) s += '\n';
      return s;
    }
  }

  function parseAll(text) {
    const docs = [[]];
    String(text).replace(/\r\n?/g, '\n').split('\n').forEach((line, i) => {
      if (/^---\s*(#.*)?$/.test(line)) { docs.push([]); return; }
      if (/^\.\.\.\s*$/.test(line)) return;
      docs[docs.length - 1].push({ text: line, no: i + 1 });
    });
    const out = [];
    for (const lines of docs) {
      const p = new Parser(lines);
      const first = p.peek();
      if (!first) continue;
      const v = p.node(first.indent);
      const rest = p.peek();
      if (rest) throw new YAMLError('unexpected content (check the indentation)', rest.no);
      if (v != null) out.push(v);
    }
    return out;
  }

  /* ---------- dumping ---------- */
  const NEEDS_QUOTE = /^$|^\d{4}-\d\d-\d\d|^[\s\-?:,[\]{}#&*!|>'"%@`]|: | #|:$|\s$|^(true|false|null|~|yes|no|on|off)$|^[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$/i;
  const fmt = v => {
    if (v === null || v === undefined) return 'null';
    if (typeof v === 'string') return NEEDS_QUOTE.test(v) ? JSON.stringify(v) : v;
    return String(v);
  };
  const isObj = v => v && typeof v === 'object' && !Array.isArray(v);

  function lines(v, ind, sort) {
    const pad = ' '.repeat(ind), out = [];
    if (Array.isArray(v)) {
      for (const item of v) {
        if (isObj(item) && Object.keys(item).length) {
          const sub = lines(item, ind + 2, sort);
          out.push(pad + '- ' + sub[0].slice(ind + 2), ...sub.slice(1));
        } else if (Array.isArray(item) && item.length) {
          out.push(pad + '-', ...lines(item, ind + 2, sort));
        } else out.push(pad + '- ' + scalarOrEmpty(item));
      }
      return out;
    }
    const keys = Object.keys(v).filter(k => v[k] !== undefined);
    if (sort) keys.sort();
    for (const k of keys) {
      const val = v[k], key = fmt(k);
      if (isObj(val) && Object.keys(val).length) out.push(pad + key + ':', ...lines(val, ind + 2, sort));
      else if (Array.isArray(val) && val.length) out.push(pad + key + ':', ...lines(val, ind, sort));
      else if (typeof val === 'string' && val.includes('\n')) {
        out.push(pad + key + ': |' + (val.endsWith('\n') ? '' : '-'));
        for (const l of val.replace(/\n$/, '').split('\n')) out.push(pad + '  ' + l);
      } else out.push(pad + key + ': ' + scalarOrEmpty(val));
    }
    return out;
  }
  function scalarOrEmpty(v) {
    if (Array.isArray(v)) return '[]';
    if (isObj(v)) return '{}';
    return fmt(v);
  }
  function dump(v, opts) {
    const sort = !!(opts && opts.sortKeys);
    if (!isObj(v) && !Array.isArray(v)) return fmt(v) + '\n';
    return lines(v, 0, sort).join('\n') + '\n';
  }

  K.yaml = { parseAll, dump, YAMLError };
})((typeof window !== 'undefined' ? window : global).SSG = (typeof window !== 'undefined' ? window : global).SSG || {});
