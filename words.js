// Word bank: words.txt (comma or newline separated). Add or remove words freely, no categories.
const fs = require('fs');
const path = require('path');
const seen = new Set();
const WORDS = [];
for (const w of fs.readFileSync(path.join(__dirname, 'words.txt'), 'utf8').split(/[\n,]/)) {
  const t = w.trim().toLowerCase();
  if (t && !seen.has(t)) { seen.add(t); WORDS.push(t); }
}
module.exports = WORDS;
