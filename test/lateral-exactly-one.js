/*
Tests for lateral selectExactlyOne enforcement (20260726_selectExactlyOne.md §8.2).

Drives runResultTransform directly with a synthetic pg.QueryResult, which is how
applyDeserializeHook is reached — so no database is needed. Plain node + assert,
because the fork has no test runner on master; port to vitest if one is added.

Runs against the BUILT output, so run `npm run build` first.

  npm test
*/
const assert = require('assert');
const db = require('../db.js');

let pass = 0, fail = 0;
const test = (name, fn) => {
  try { fn(); pass++; console.log('  \x1b[32mPASS\x1b[0m ' + name); }
  catch (e) { fail++; console.log('  \x1b[31mFAIL\x1b[0m ' + name + '\n        ' + e.message); }
};
const qr = (result) => ({ rows: result === undefined ? [] : [{ result }] });

// --- fixtures -------------------------------------------------------------
const exactlyOneChild = () => db.selectExactlyOne('child', { parent_id: db.parent('id') });
const oneChild = () => db.selectOne('child', { parent_id: db.parent('id') });
const manyChildren = () => db.select('child', { parent_id: db.parent('id') });
const parentWith = (lateral) => db.selectOne('parent', db.all, { lateral });

console.log('\n== the fix ==');

test('1. lateral ExactlyOne + NULL throws NotExactlyOneError', () => {
  const q = parentWith({ child: exactlyOneChild() });
  assert.throws(() => q.runResultTransform(qr({ id: 1, child: null })), db.NotExactlyOneError);
});

test('2. lateral ExactlyOne + value returns it, deserialize hook applied', () => {
  db.registerDeserializeHook('child', 'amount', (x) => 'HOOKED:' + x);
  const q = parentWith({ child: exactlyOneChild() });
  const out = q.runResultTransform(qr({ id: 1, child: { amount: 42 } }));
  assert.strictEqual(out.child.amount, 'HOOKED:42');
});

test('3. lateral selectOne + NULL does NOT throw (must not regress)', () => {
  const q = parentWith({ child: oneChild() });
  const out = q.runResultTransform(qr({ id: 1, child: null }));
  assert.strictEqual(out.child, null);
});

test('4. lateral select (Many) + empty array does NOT throw', () => {
  const q = parentWith({ child: manyChildren() });
  const out = q.runResultTransform(qr({ id: 1, child: [] }));
  assert.deepStrictEqual(out.child, []);
});

test('5. nested lateral: inner ExactlyOne NULL two levels down throws', () => {
  const inner = db.selectOne('child', db.all, { lateral: { grandchild: exactlyOneChild() } });
  const q = parentWith({ child: inner });
  assert.throws(
    () => q.runResultTransform(qr({ id: 1, child: { id: 2, grandchild: null } })),
    db.NotExactlyOneError,
  );
});

test('7. outermost ExactlyOne with no rows still throws (control: API works at top level)', () => {
  const q = db.selectExactlyOne('parent', db.all);
  assert.throws(() => q.runResultTransform(qr(undefined)), db.NotExactlyOneError);
});

test('8. message names the lateral key and parent table; .query.compile() works', () => {
  const q = parentWith({ child: exactlyOneChild() });
  try {
    q.runResultTransform(qr({ id: 1, child: null }));
    assert.fail('expected throw');
  } catch (e) {
    assert.ok(e instanceof db.NotExactlyOneError, 'is NotExactlyOneError');
    assert.ok(/lateral 'child'/.test(e.message), "names lateral key, got: " + e.message);
    assert.ok(/on 'parent'/.test(e.message), "names parent table, got: " + e.message);
    assert.ok(e.query && typeof e.query.compile === 'function', 'carries a compilable query');
    assert.ok(e.query.compile().text.length > 0, '.query.compile() produces SQL');
  }
});

console.log('\n== things that must not be mistaken for a missing row ==');

test('lateral count of 0 is not treated as absent (falsy, but present)', () => {
  const q = parentWith({ n: db.count('child', { parent_id: db.parent('id') }) });
  const out = q.runResultTransform(qr({ id: 1, n: 0 }));
  assert.strictEqual(out.n, 0);
});

console.log('\n== regression guards ==');

// NB: 20260726_selectExactlyOne.md §3.4 claims passthru laterals already enforce
// ExactlyOne. They did not — applyDeserializeHook returns early on `!values`, before
// the passthru branch that would call the sub-query's transform. But unlike the keyed
// form it cannot be fixed there either, because by that point `null` (lateral missed)
// and `undefined` (parent missed) have collapsed into one falsy branch. The passthru
// form is therefore enforced one level up, in runResultTransform, where `qr` is still
// in scope. These two tests are the pair that pins that distinction: a fix that checks
// "is the result null?" without also checking whether a row came back passes the first
// and breaks the second.
test('6a. passthru lateral: parent present, ExactlyOne matched nothing -> throws', () => {
  const q = db.selectOne('parent', db.all, { lateral: db.selectExactlyOne('child', db.all) });
  assert.throws(() => q.runResultTransform(qr(null)), db.NotExactlyOneError);
});

test('6b. passthru lateral: outer read matching nothing -> undefined, no throw', () => {
  const q = db.selectOne('parent', db.all, { lateral: db.selectExactlyOne('child', db.all) });
  assert.strictEqual(q.runResultTransform(qr(undefined)), undefined);
});

test('6c. passthru lateral: match returns the lateral row, no throw', () => {
  const q = db.selectOne('parent', db.all, { lateral: db.selectExactlyOne('child', db.all) });
  assert.deepStrictEqual(q.runResultTransform(qr({ id: 1 })), { id: 1 });
});

test('6d. passthru under selectExactlyOne: null result throws rather than returning null', () => {
  // The outermost guard only tests `result === undefined`, so before the fix a passthru
  // miss made selectExactlyOne *return null* — the top-level API breaking its own contract.
  const q = db.selectExactlyOne('parent', db.all, { lateral: db.selectExactlyOne('child', db.all) });
  assert.throws(() => q.runResultTransform(qr(null)), db.NotExactlyOneError);
});

test('6e. passthru under select (Many): a null element throws', () => {
  // Many aggregates the rows, so a miss is a null *element*, not a null result.
  const q = db.select('parent', db.all, { lateral: db.selectExactlyOne('child', db.all) });
  assert.throws(() => q.runResultTransform(qr([{ id: 1 }, null])), db.NotExactlyOneError);
  assert.deepStrictEqual(q.runResultTransform(qr([{ id: 1 }])), [{ id: 1 }]);
  assert.deepStrictEqual(q.runResultTransform(qr([])), []);
});

test('6f. passthru error carries a query that actually compiles', () => {
  const q = db.selectOne('parent', db.all, { lateral: db.selectExactlyOne('child', db.all) });
  try {
    q.runResultTransform(qr(null));
    assert.fail('expected throw');
  } catch (e) {
    assert.ok(e instanceof db.NotExactlyOneError, 'is NotExactlyOneError');
    assert.ok(/on 'parent'/.test(e.message), 'names the parent table, got: ' + e.message);
    assert.ok(e.query.compile().text.length > 0, '.query.compile() produces SQL');
  }
});

test('6g. passthru lateral that is NOT ExactlyOne still tolerates a miss', () => {
  const q = db.selectOne('parent', db.all, { lateral: db.selectOne('child', db.all) });
  assert.strictEqual(q.runResultTransform(qr(null)), null);
});

// A passthru nested inside a *keyed* lateral cannot be enforced: the parent's column
// is the inner query's result column, so null means either "inner parent missing"
// (legal) or "innermost lateral missing" (a violation), with nothing in the result set
// to separate them. Pinned as a known limit, not as desired behaviour.
test('6h. LIMIT: passthru nested inside a keyed lateral is not enforceable', () => {
  const q = db.selectOne('parent', db.all, {
    lateral: { kid: db.selectOne('child', db.all, { lateral: db.selectExactlyOne('grandchild', db.all) }) },
  });
  assert.deepStrictEqual(q.runResultTransform(qr({ id: 1, kid: null })), { id: 1, kid: null });
});

test('6i. select (Many) with a null element does not crash the serde walk', () => {
  // Pre-existing: Object.entries(null) threw a bare TypeError out of applyHookSingle,
  // for any passthru lateral mode, not just ExactlyOne.
  const q = db.select('parent', db.all, { lateral: db.selectOne('child', db.all) });
  assert.deepStrictEqual(q.runResultTransform(qr([{ id: 1 }, null])), [{ id: 1 }, null]);
});

test('selectResultMode survives .copy() (upstream 6e64759 defensive copy)', () => {
  const sub = exactlyOneChild();
  assert.strictEqual(sub.copy({ parentTable: 'x' }).selectResultMode, db.SelectResultMode.ExactlyOne);
});

test('SelectResultMode / NotExactlyOneError still on the public surface after the move', () => {
  assert.strictEqual(typeof db.NotExactlyOneError, 'function');
  assert.strictEqual(db.SelectResultMode.ExactlyOne, 2);
});

console.log('\n' + (fail ? '\x1b[31m' : '\x1b[32m') + pass + ' passed, ' + fail + ' failed\x1b[0m\n');
process.exit(fail ? 1 : 0);
