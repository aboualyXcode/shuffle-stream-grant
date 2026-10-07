/* grant/uc.js: a model of Unity Catalog access control (privilege model 1.0).
   - Securables are hierarchical: catalog > schema > table | view | volume | function.
   - Privileges are inherited downward: SELECT granted on a schema applies to every current and
     future table in it.
   - Reading a table needs USE CATALOG on its catalog, USE SCHEMA on its schema, and SELECT on it
     (each held directly or inherited). USE SCHEMA is required but not sufficient.
   - Owners hold all privileges on the object they own, not on its children, and still need the
     USE privileges on its parents.
   - Querying a view needs SELECT on the view; the view reads its base tables with its owner's rights.
   - Row filters and column masks are SQL functions evaluated for the user running the query. */
(function (root) {
  'use strict';
  const SSG = root.SSG = root.SSG || {};

  class UcError extends Error { constructor(cls, msg) { super(`[${cls}] ${msg}`); this.cls = cls; } }
  const PRIVS = {
    CATALOG: ['USE CATALOG', 'USE SCHEMA', 'CREATE SCHEMA', 'CREATE TABLE', 'CREATE FUNCTION', 'CREATE VOLUME', 'SELECT', 'MODIFY', 'EXECUTE', 'READ VOLUME', 'WRITE VOLUME', 'BROWSE', 'APPLY TAG', 'MANAGE'],
    SCHEMA: ['USE SCHEMA', 'CREATE TABLE', 'CREATE FUNCTION', 'CREATE VOLUME', 'SELECT', 'MODIFY', 'EXECUTE', 'READ VOLUME', 'WRITE VOLUME', 'BROWSE', 'APPLY TAG', 'MANAGE'],
    TABLE: ['SELECT', 'MODIFY', 'APPLY TAG', 'MANAGE'],
    VIEW: ['SELECT', 'APPLY TAG', 'MANAGE'],
    VOLUME: ['READ VOLUME', 'WRITE VOLUME', 'MANAGE'],
    FUNCTION: ['EXECUTE', 'MANAGE'],
  };

  /* ---------- expressions for row filters and masks ---------- */
  function tokenize(s) {
    const out = []; let i = 0;
    while (i < s.length) {
      const c = s[i];
      if (/\s/.test(c)) { i++; continue; }
      if (c === "'") { let j = i + 1, v = ''; while (j < s.length && s[j] !== "'") v += s[j++]; out.push({ t: 'str', v }); i = j + 1; continue; }
      if (c === '`') { const j = s.indexOf('`', i + 1); out.push({ t: 'id', v: s.slice(i + 1, j) }); i = j + 1; continue; }
      if (/[0-9]/.test(c)) { const m = /^\d+(\.\d+)?/.exec(s.slice(i))[0]; out.push({ t: 'num', v: Number(m) }); i += m.length; continue; }
      if (/[A-Za-z_]/.test(c)) { const m = /^[A-Za-z_][A-Za-z0-9_.]*/.exec(s.slice(i))[0]; out.push({ t: 'id', v: m, u: m.toUpperCase() }); i += m.length; continue; }
      const op = ['<>', '!=', '>=', '<=', '||', '=', '<', '>', '(', ')', ','].find(o => s.startsWith(o, i));
      if (!op) throw new UcError('PARSE_SYNTAX_ERROR', `Syntax error at or near '${c}'.`);
      out.push({ t: 'op', v: op }); i += op.length;
    }
    out.push({ t: 'eof' });
    return out;
  }
  function parseExpr(src) {
    const t = tokenize(src); let i = 0;
    const kw = w => t[i].t === 'id' && t[i].u === w;
    const op = o => t[i].t === 'op' && t[i].v === o;
    const expect = o => { if (!op(o)) throw new UcError('PARSE_SYNTAX_ERROR', `Syntax error: expected '${o}'.`); i++; };
    const or = () => { let l = and(); while (kw('OR')) { i++; l = { k: 'or', l, r: and() }; } return l; };
    const and = () => { let l = not(); while (kw('AND')) { i++; l = { k: 'and', l, r: not() }; } return l; };
    const not = () => { if (kw('NOT')) { i++; return { k: 'not', e: not() }; } return cmp(); };
    const cmp = () => {
      let l = cat();
      if (kw('IS')) { i++; const n = kw('NOT') ? (i++, true) : false; if (!kw('NULL')) throw new UcError('PARSE_SYNTAX_ERROR', 'Syntax error: expected NULL.'); i++; return { k: 'isnull', e: l, not: n }; }
      const neg = kw('NOT') && t[i + 1].t === 'id' && t[i + 1].u === 'IN'; if (neg) i++;
      if (kw('IN')) { i++; expect('('); const list = [or()]; while (op(',')) { i++; list.push(or()); } expect(')'); return { k: 'in', e: l, list, not: neg }; }
      for (const o of ['=', '<>', '!=', '>=', '<=', '<', '>']) if (op(o)) { i++; return { k: 'cmp', op: o === '!=' ? '<>' : o, l, r: cat() }; }
      return l;
    };
    const cat = () => { let l = prim(); while (op('||')) { i++; l = { k: 'concat', args: [l, prim()] }; } return l; };
    const prim = () => {
      const tok = t[i];
      if (tok.t === 'str') { i++; return { k: 'lit', v: tok.v }; }
      if (tok.t === 'num') { i++; return { k: 'lit', v: tok.v }; }
      if (op('(')) { i++; const e = or(); expect(')'); return e; }
      if (tok.t !== 'id') throw new UcError('PARSE_SYNTAX_ERROR', 'Syntax error in expression.');
      if (tok.u === 'TRUE' || tok.u === 'FALSE') { i++; return { k: 'lit', v: tok.u === 'TRUE' }; }
      if (tok.u === 'NULL') { i++; return { k: 'lit', v: null }; }
      if (tok.u === 'CASE') {
        i++; const whens = [];
        while (kw('WHEN')) { i++; const c = or(); if (!kw('THEN')) throw new UcError('PARSE_SYNTAX_ERROR', 'Syntax error: expected THEN.'); i++; whens.push([c, or()]); }
        let els = { k: 'lit', v: null }; if (kw('ELSE')) { i++; els = or(); }
        if (!kw('END')) throw new UcError('PARSE_SYNTAX_ERROR', 'Syntax error: expected END.'); i++;
        return { k: 'case', whens, els };
      }
      i++;
      if (op('(')) { i++; const args = []; if (!op(')')) { args.push(or()); while (op(',')) { i++; args.push(or()); } } expect(')'); return { k: 'fn', name: tok.v.toLowerCase(), args }; }
      return { k: 'ref', name: tok.v.toLowerCase() };
    };
    const e = or();
    if (t[i].t !== 'eof') throw new UcError('PARSE_SYNTAX_ERROR', 'Syntax error: unexpected text after the expression.');
    return e;
  }
  const FNS = {
    right: (s, n) => (s == null ? null : String(s).slice(-n)),
    left: (s, n) => (s == null ? null : String(s).slice(0, n)),
    concat: (...a) => (a.some(x => x == null) ? null : a.join('')),
    upper: s => (s == null ? null : String(s).toUpperCase()),
    lower: s => (s == null ? null : String(s).toLowerCase()),
    length: s => (s == null ? null : String(s).length),
    repeat: (s, n) => (s == null ? null : String(s).repeat(n)),
    coalesce: (...a) => { for (const x of a) if (x != null) return x; return null; },
    if: (c, a, b) => (c === true ? a : b),
    split_part: (s, d, n) => (s == null ? null : (String(s).split(d)[n - 1] || '')),
  };
  function evalExpr(e, env, ctx) {
    switch (e.k) {
      case 'lit': return e.v;
      case 'ref': if (!(e.name in env)) throw new UcError('UNRESOLVED_COLUMN', `A column or parameter named \`${e.name}\` cannot be resolved.`); return env[e.name];
      case 'or': { const a = evalExpr(e.l, env, ctx); if (a === true) return true; const b = evalExpr(e.r, env, ctx); return b === true ? true : a == null || b == null ? null : false; }
      case 'and': { const a = evalExpr(e.l, env, ctx); if (a === false) return false; const b = evalExpr(e.r, env, ctx); return b === false ? false : a == null || b == null ? null : true; }
      case 'not': { const v = evalExpr(e.e, env, ctx); return v == null ? null : !v; }
      case 'isnull': return (evalExpr(e.e, env, ctx) == null) !== e.not;
      case 'in': { const v = evalExpr(e.e, env, ctx); if (v == null) return null; const hit = e.list.some(x => evalExpr(x, env, ctx) === v); return e.not ? !hit : hit; }
      case 'cmp': {
        const a = evalExpr(e.l, env, ctx), b = evalExpr(e.r, env, ctx);
        if (a == null || b == null) return null;
        return { '=': a === b, '<>': a !== b, '<': a < b, '>': a > b, '<=': a <= b, '>=': a >= b }[e.op];
      }
      case 'concat': { const v = e.args.map(x => evalExpr(x, env, ctx)); return v.some(x => x == null) ? null : v.join(''); }
      case 'case': for (const [c, v] of e.whens) if (evalExpr(c, env, ctx) === true) return evalExpr(v, env, ctx); return evalExpr(e.els, env, ctx);
      case 'fn': {
        if (e.name === 'is_account_group_member' || e.name === 'is_member') return ctx.uc.isMember(ctx.user, evalExpr(e.args[0], env, ctx));
        if (e.name === 'current_user' || e.name === 'session_user') return ctx.user;
        const f = FNS[e.name];
        if (!f) throw new UcError('UNRESOLVED_ROUTINE', `Cannot resolve routine \`${e.name}\`. Supported here: is_account_group_member, current_user, ${Object.keys(FNS).join(', ')}.`);
        return f(...e.args.map(x => evalExpr(x, env, ctx)));
      }
      default: throw new UcError('INTERNAL_ERROR', e.k);
    }
  }

  /* ---------- the catalog ---------- */
  class UnityCatalog {
    constructor() {
      this.objects = new Map();   // full name -> {type, name, owner, ...}
      this.principals = new Map(); // name -> {kind: user|group|sp, member of groups}
      this.grants = new Set();     // "TYPE|object|principal|PRIVILEGE"
      this.functions = new Map();
      this.history = [];
      this.principals.set('account users', { kind: 'group', groups: [], builtin: true }); // every user and service principal
    }
    /* principals */
    addUser(name, groups) { this.principals.set(name, { kind: 'user', groups: groups || [] }); }
    addServicePrincipal(name, groups) { this.principals.set(name, { kind: 'service principal', groups: groups || [] }); }
    addGroup(name, groups) { this.principals.set(name, { kind: 'group', groups: groups || [] }); }
    groupsOf(p) {
      const out = new Set(), stack = [p];
      while (stack.length) { const x = stack.pop(); const pr = this.principals.get(x); if (!pr) continue; pr.groups.forEach(g => { if (!out.has(g)) { out.add(g); stack.push(g); } }); }
      if (this.principals.has(p) && this.principals.get(p).kind !== 'group') out.add('account users');
      return out;
    }
    isMember(user, group) { return this.groupsOf(user).has(group); }
    identities(p) { return new Set([p, ...this.groupsOf(p)]); }

    /* securables */
    add(type, name, props) { const o = Object.assign({ type, name, owner: 'data-platform', cols: [], rows: [] }, props || {}); this.objects.set(name.toLowerCase(), o); return o; }
    get(name) { return this.objects.get(String(name).toLowerCase()) || null; }
    parentsOf(name) { const p = name.split('.'); return p.length === 3 ? [p[0], p[0] + '.' + p[1]] : p.length === 2 ? [p[0]] : []; }

    /* Has `principal` got `priv` on object (directly, through a group, inherited, or as owner)? */
    holds(principal, priv, name) {
      const ids = this.identities(principal);
      const chain = this.parentsOf(name).concat([name]);
      for (const n of chain) {
        const o = this.get(n);
        if (!o) continue;
        for (const id of ids) {
          if (this.grants.has(`${n.toLowerCase()}|${id}|${priv}`) || this.grants.has(`${n.toLowerCase()}|${id}|ALL PRIVILEGES`)) return true;
        }
      }
      const self = this.get(name);
      if (self && ids.has(self.owner) && (PRIVS[self.type] || []).includes(priv)) return true;
      return false;
    }
    missing(principal, name, priv) {
      const o = this.get(name);
      if (!o) throw new UcError('TABLE_OR_VIEW_NOT_FOUND', `The table or view \`${name}\` cannot be found.`);
      const parts = name.split('.');
      const need = [];
      if (!this.holds(principal, 'USE CATALOG', parts[0])) need.push(['USE CATALOG', 'Catalog', parts[0]]);
      if (parts.length >= 2 && !this.holds(principal, 'USE SCHEMA', parts.slice(0, 2).join('.'))) need.push(['USE SCHEMA', 'Schema', parts.slice(0, 2).join('.')]);
      if (priv && !this.holds(principal, priv, name)) need.push([priv, { TABLE: 'Table', VIEW: 'View', SCHEMA: 'Schema', CATALOG: 'Catalog', VOLUME: 'Volume', FUNCTION: 'Function' }[o.type], name]);
      return need;
    }
    deny(principal, need) {
      const [p, kind, n] = need[0];
      const who = this.principals.get(principal);
      return new UcError('INSUFFICIENT_PERMISSIONS', `Insufficient privileges: ${who && who.kind === 'service principal' ? 'Service principal' : 'User'} ${principal} does not have ${p} on ${kind} '${n}'.`);
    }

    /* ---------- actions ---------- */
    /* SELECT * FROM name, as `user`. Returns rows with row filters and masks applied. */
    select(user, name, viaOwner) {
      const o = this.get(name);
      if (!o) throw new UcError('TABLE_OR_VIEW_NOT_FOUND', `The table or view \`${name}\` cannot be found. Verify the spelling and correctness of the schema and catalog.`);
      const actor = viaOwner || user;
      const need = this.missing(actor, name, 'SELECT');
      if (need.length) throw this.deny(actor, need);
      if (o.type === 'VIEW') {
        // the view's query runs with the view owner's privileges; filters and masks still see the caller
        let rows;
        try { rows = this.select(user, o.from, o.owner); }
        catch (e) { if (e.cls === 'INSUFFICIENT_PERMISSIONS') throw new UcError('INSUFFICIENT_PERMISSIONS', `The view \`${name}\` cannot be read because its owner (${o.owner}) lacks access to its base table: ${e.message.replace(/^\[[A-Z_]+\] /, '')}`); throw e; }
        const ctx = { uc: this, user };
        if (o.where) rows = rows.filter(r => evalExpr(o.where, lowerKeys(r), ctx) === true);
        if (o.cols) rows = rows.map(r => { const out = {}; o.cols.forEach(c => { out[c.as] = c.expr ? evalExpr(c.expr, lowerKeys(r), ctx) : r[c.as]; }); return out; });
        return rows;
      }
      const ctx = { uc: this, user };
      let rows = o.rows.map(r => Object.assign({}, r));
      if (o.rowFilter) {
        const f = this.functions.get(o.rowFilter.fn.toLowerCase());
        rows = rows.filter(r => { const env = {}; f.params.forEach((p, i) => { env[p] = r[o.rowFilter.cols[i]]; }); return evalExpr(f.body, env, ctx) === true; });
      }
      Object.entries(o.masks || {}).forEach(([col, m]) => {
        const f = this.functions.get(m.fn.toLowerCase());
        rows.forEach(r => { const env = {}; env[f.params[0]] = r[col]; (m.using || []).forEach((u, i) => { env[f.params[i + 1]] = r[u]; }); r[col] = evalExpr(f.body, env, ctx); });
      });
      return rows;
    }
    can(user, action, name) {
      try {
        if (action === 'select') { this.select(user, name); return { ok: true }; }
        const req = { modify: 'MODIFY', create_table: 'CREATE TABLE', read_volume: 'READ VOLUME', write_volume: 'WRITE VOLUME', execute: 'EXECUTE', create_schema: 'CREATE SCHEMA' }[action];
        const o = this.get(name);
        if (!o) throw new UcError('TABLE_OR_VIEW_NOT_FOUND', `\`${name}\` cannot be found.`);
        if (action === 'modify' && o.type === 'VIEW') throw new UcError('EXPECT_TABLE_NOT_VIEW', `\`${name}\` is a view; views cannot be modified.`);
        const need = this.missing(user, name, req);
        if (need.length) throw this.deny(user, need);
        return { ok: true };
      } catch (e) { return { ok: false, error: e.message, cls: e.cls }; }
    }

    /* ---------- statements ---------- */
    execute(sql, as) {
      const out = [];
      for (const stmt of splitStatements(sql)) out.push(this.statement(stmt, as || 'admin'));
      return out;
    }
    statement(s, as) {
      let m;
      const txt = s.replace(/\s+/g, ' ').trim();
      if ((m = /^(GRANT|REVOKE) (.+?) ON (CATALOG|SCHEMA|TABLE|VIEW|VOLUME|FUNCTION) (\S+) (TO|FROM) (.+)$/i.exec(txt))) {
        const [, verb, privList, typeRaw, objRaw, dir, whoRaw] = m;
        if ((verb.toUpperCase() === 'GRANT') !== (dir.toUpperCase() === 'TO')) throw new UcError('PARSE_SYNTAX_ERROR', `${verb.toUpperCase()} uses ${verb.toUpperCase() === 'GRANT' ? 'TO' : 'FROM'}.`);
        const type = typeRaw.toUpperCase(), name = unq(objRaw);
        const o = this.get(name);
        if (!o) throw new UcError(type === 'CATALOG' ? 'NO_SUCH_CATALOG_EXCEPTION' : type === 'SCHEMA' ? 'SCHEMA_NOT_FOUND' : 'TABLE_OR_VIEW_NOT_FOUND', `${type[0] + type.slice(1).toLowerCase()} '${name}' not found.`);
        const realType = o.type;
        if (type !== realType && !(type === 'TABLE' && realType === 'VIEW')) throw new UcError('INVALID_SECURABLE_TYPE', `'${name}' is a ${realType.toLowerCase()}, not a ${type.toLowerCase()}. Use ON ${realType} ${name}.`);
        const who = unq(whoRaw.trim());
        if (!this.principals.has(who)) throw new UcError('PRINCIPAL_DOES_NOT_EXIST', `Could not find principal with name ${who}.`);
        const privs = privList.split(',').map(p => p.trim().toUpperCase().replace(/\s+/g, ' ')).map(p => (p === 'ALL' ? 'ALL PRIVILEGES' : p));
        for (const p of privs) {
          if (p !== 'ALL PRIVILEGES' && !(PRIVS[realType] || []).includes(p)) throw new UcError('INVALID_PRIVILEGE', `Privilege ${p} is not valid for ${realType.toLowerCase()}. Valid privileges: ${PRIVS[realType].join(', ')}, ALL PRIVILEGES.`);
          const key = `${name.toLowerCase()}|${who}|${p}`;
          if (verb.toUpperCase() === 'GRANT') this.grants.add(key); else this.grants.delete(key);
        }
        this.history.push(txt);
        return { ok: true, message: `${verb.toUpperCase() === 'GRANT' ? 'Granted' : 'Revoked'} ${privs.join(', ')} on ${realType.toLowerCase()} ${name} ${verb.toUpperCase() === 'GRANT' ? 'to' : 'from'} ${who}.` };
      }
      if ((m = /^CREATE (?:OR REPLACE )?FUNCTION (\S+?)\s*\((.*?)\)\s*(?:RETURNS\s+\w+(?:\([^)]*\))?\s+)?RETURN\s+(.+)$/i.exec(txt))) {
        const [, fname, params, body] = m;
        const ps = params.split(',').map(p => p.trim().split(/\s+/)[0]).filter(Boolean).map(p => p.toLowerCase());
        this.functions.set(unq(fname).toLowerCase(), { name: unq(fname), params: ps, body: parseExpr(body) });
        if (!this.get(unq(fname))) this.add('FUNCTION', unq(fname), { owner: as });
        return { ok: true, message: `Function ${unq(fname)} created.` };
      }
      if ((m = /^ALTER TABLE (\S+) SET ROW FILTER (\S+) ON \((.*)\)$/i.exec(txt))) {
        const o = this.tableFor(m[1]); const f = this.fnFor(m[2]);
        const cols = m[3].split(',').map(c => c.trim().toLowerCase());
        if (cols.length !== f.params.length) throw new UcError('ROW_FILTER_ARITY', `Function ${f.name} takes ${f.params.length} parameter(s), but ${cols.length} column(s) were given.`);
        cols.forEach(c => { if (!o.cols.includes(c)) throw new UcError('UNRESOLVED_COLUMN', `Column \`${c}\` does not exist in ${o.name}.`); });
        o.rowFilter = { fn: f.name, cols };
        return { ok: true, message: `Row filter ${f.name} set on ${o.name}.` };
      }
      if ((m = /^ALTER TABLE (\S+) DROP ROW FILTER$/i.exec(txt))) { this.tableFor(m[1]).rowFilter = null; return { ok: true, message: 'Row filter dropped.' }; }
      if ((m = /^ALTER TABLE (\S+) ALTER COLUMN (\S+) SET MASK (\S+)(?: USING COLUMNS \((.*)\))?$/i.exec(txt))) {
        const o = this.tableFor(m[1]); const col = unq(m[2]).toLowerCase(); const f = this.fnFor(m[3]);
        if (!o.cols.includes(col)) throw new UcError('UNRESOLVED_COLUMN', `Column \`${col}\` does not exist in ${o.name}.`);
        const using = m[4] ? m[4].split(',').map(c => c.trim().toLowerCase()) : [];
        if (f.params.length !== 1 + using.length) throw new UcError('MASK_ARITY', `Mask function ${f.name} takes ${f.params.length} parameter(s): the masked column${f.params.length > 1 ? ' plus USING COLUMNS' : ''}.`);
        o.masks = Object.assign({}, o.masks, { [col]: { fn: f.name, using } });
        return { ok: true, message: `Mask ${f.name} set on ${o.name}.${col}.` };
      }
      if ((m = /^ALTER TABLE (\S+) ALTER COLUMN (\S+) DROP MASK$/i.exec(txt))) { const o = this.tableFor(m[1]); delete (o.masks || {})[unq(m[2]).toLowerCase()]; return { ok: true, message: 'Mask dropped.' }; }
      if ((m = /^ALTER (CATALOG|SCHEMA|TABLE|VIEW|VOLUME) (\S+) (?:SET )?OWNER TO (.+)$/i.exec(txt))) {
        const o = this.get(unq(m[2])); if (!o) throw new UcError('TABLE_OR_VIEW_NOT_FOUND', `'${m[2]}' not found.`);
        const who = unq(m[3].trim()); if (!this.principals.has(who)) throw new UcError('PRINCIPAL_DOES_NOT_EXIST', `Could not find principal with name ${who}.`);
        o.owner = who; return { ok: true, message: `Owner of ${o.name} is now ${who}.` };
      }
      if ((m = /^SHOW GRANTS(?: (\S+))? ON (CATALOG|SCHEMA|TABLE|VIEW|VOLUME) (\S+)$/i.exec(txt))) {
        const name = unq(m[3]).toLowerCase(), who = m[1] ? unq(m[1]) : null;
        const rows = Array.from(this.grants).map(g => g.split('|')).filter(([o, p]) => o === name && (!who || p === who)).map(([o, p, pr]) => ({ principal: p, privilege: pr, object_type: m[2].toUpperCase(), object_key: o }));
        return { ok: true, rows };
      }
      throw new UcError('PARSE_SYNTAX_ERROR', `Not supported in this lab: "${txt.slice(0, 60)}${txt.length > 60 ? '…' : ''}". Supported: GRANT, REVOKE, CREATE FUNCTION, ALTER TABLE … SET ROW FILTER / ALTER COLUMN … SET MASK, ALTER … OWNER TO, SHOW GRANTS.`);
    }
    tableFor(n) { const o = this.get(unq(n)); if (!o || o.type !== 'TABLE') throw new UcError('TABLE_OR_VIEW_NOT_FOUND', `Table '${n}' not found.`); return o; }
    fnFor(n) { const f = this.functions.get(unq(n).toLowerCase()); if (!f) throw new UcError('ROUTINE_NOT_FOUND', `Function '${n}' not found. Create it first with CREATE FUNCTION.`); return f; }
    grantList() { return Array.from(this.grants).map(g => { const [object, principal, privilege] = g.split('|'); return { object, principal, privilege, type: (this.get(object) || {}).type }; }); }
  }
  const unq = s => String(s).replace(/^`|`$/g, '').replace(/^'|'$/g, '');
  const lowerKeys = r => { const o = {}; Object.keys(r).forEach(k => { o[k.toLowerCase()] = r[k]; }); return o; };
  function splitStatements(sql) {
    const out = []; let cur = '', q = null, depth = 0;
    const src = sql.replace(/--[^\n]*/g, '');
    for (const c of src) {
      if (q) { cur += c; if (c === q) q = null; continue; }
      if (c === "'" || c === '`') { q = c; cur += c; continue; }
      if (c === '(') depth++; if (c === ')') depth--;
      if (c === ';' && depth === 0) { if (cur.trim()) out.push(cur.trim()); cur = ''; continue; }
      cur += c;
    }
    if (cur.trim()) out.push(cur.trim());
    return out;
  }

  SSG.uc = { UnityCatalog, UcError, parseExpr, evalExpr, PRIVS, splitStatements };
})(typeof window !== 'undefined' ? window : global);
