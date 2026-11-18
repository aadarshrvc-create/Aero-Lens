// Every element that index.html hides with `hidden` must actually be hidden.
// A bare `display:` rule on the element or one of its classes beats the UA
// stylesheet's [hidden] rule, which is how both <main> tabs ended up on screen
// at the same time.
const fs = require('fs');
const path = require('path');

const STATIC = path.join(__dirname, '..', 'webapp', 'static');
const html = fs.readFileSync(path.join(STATIC, 'index.html'), 'utf8');
// Comments must go first, or they get captured as part of the next selector.
const css = fs.readFileSync(path.join(STATIC, 'styles.css'), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');

const override = /\[hidden\]\s*\{[^}]*display:\s*none\s*!important/.test(css);

// selectors that set `display` without !important
const displaySetters = new Set();
for (const block of css.matchAll(/([^{}]+)\{([^}]*)\}/g)) {
  const [, selectors, body] = block;
  if (!/display:\s*(?!none\s*!important)[^;]+/.test(body)) continue;
  selectors.split(',').forEach((sel) => displaySetters.add(sel.trim()));
}

const hiddenTags = [...html.matchAll(/<(\w+)([^>]*\shidden[\s>])[^>]*>/g)];
const problems = [];
for (const [tag, attrs] of hiddenTags.map((m) => [m[1], m[0]])) {
  const id = (attrs.match(/id="([^"]+)"/) || [])[1];
  const classes = ((attrs.match(/class="([^"]+)"/) || [])[1] || '').split(/\s+/).filter(Boolean);
  const matched = [];
  if (displaySetters.has(tag)) matched.push(tag);
  if (id && displaySetters.has(`#${id}`)) matched.push(`#${id}`);
  classes.forEach((c) => { if (displaySetters.has(`.${c}`)) matched.push(`.${c}`); });
  if (matched.length) problems.push({ element: id ? `#${id}` : `<${tag}>`, beatenBy: matched });
}

console.log(`[hidden] { display: none !important } present: ${override}`);
console.log(`elements using the hidden attribute: ${hiddenTags.length}`);
if (!problems.length) {
  console.log('none of them have a competing display rule');
} else {
  problems.forEach((p) => console.log(`  ${p.element} has display set by: ${p.beatenBy.join(', ')}`));
  if (!override) {
    console.error('\nFAIL: these elements will stay visible when hidden is set');
    process.exit(1);
  }
  console.log('\nall neutralised by the !important override');
}
