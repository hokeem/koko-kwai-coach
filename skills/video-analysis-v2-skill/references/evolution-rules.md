# Evolution Rules

## Purpose

Store stable SOP-level improvements learned from repeated comparison between:
- the agent's own analysis
- human-approved good examples

Only keep reusable rules here.
Do not dump one-off case notes into this file.

---

## Rule format

For each confirmed rule, record:
- `problem`
- `failure_cause`
- `improvement_action`
- `applies_when`
- `sop_target`

Keep each rule short.

---

## Example shape

### Rule 1
- `problem`: dialogue can be transcribed, but speaker ownership is unstable
- `failure_cause`: the workflow segments text but does not explicitly assign speaking roles
- `improvement_action`: add a speaker-attribution pass before final dialogue writing
- `applies_when`: two-person dialogue, repeated back-and-forth, interruptions, emotionally reactive exchanges
- `sop_target`: Part 3.2 / Part 4.4

---

## Admission rule

Only add a rule when:
- the same problem appears more than once, or
- one comparison clearly reveals a reusable method improvement rather than a one-off correction

If the insight belongs to a special video type rather than the general SOP, store it in `references/special-video-patterns.md` instead.
