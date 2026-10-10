// PouchDB side of the protocol interop test (see PouchInteropTest.kt).
// Usage: node pouch.mjs read <db-url> <id>
//        node pouch.mjs write <db-url> <id> <value>
import PouchDB from "pouchdb-node";

const [action, url, id, value] = process.argv.slice(2);
const remote = new PouchDB(url, { skip_setup: true });
const local = new PouchDB(`memory-${Date.now()}`);

if (action === "read") {
  await local.replicate.from(remote);
  const doc = await local.get(id);
  console.log(JSON.stringify({ rev: doc._rev, v: doc.v }));
} else if (action === "write") {
  await local.replicate.from(remote);
  const existing = await local.get(id).catch(() => null);
  await local.put({ _id: id, v: value, ...(existing ? { _rev: existing._rev } : {}) });
  await local.replicate.to(remote);
  console.log(JSON.stringify({ ok: true }));
} else {
  console.error(`unknown action: ${action}`);
  process.exit(2);
}
await local.close();
await remote.close();
