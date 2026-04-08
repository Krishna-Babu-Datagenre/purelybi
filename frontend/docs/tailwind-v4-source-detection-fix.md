# Tailwind v4: Missing Utility Classes Due to `.gitignore`

## Problem

Many Tailwind utility classes silently failed to generate — no errors, no warnings.
Classes like `grid-cols-2`, `sm:grid-cols-3`, `space-y-6`, `pl-11` were completely absent
from the built CSS, while others like `gap-3`, `rounded-xl`, `text-sm` worked fine.

Responsive breakpoint prefixes (`sm:`, `md:`, `lg:`) appeared broken because no responsive variant rules were generated for any class used exclusively in the affected files.

## Root Cause

Tailwind v4 automatically detects which source files to scan for class names.
It respects **all `.gitignore` files in the directory tree**, not just the one in
the project root.

The **parent monorepo** `.gitignore` at `bi-agent-2.0/.gitignore` contained a bare rule:

```gitignore
data
```

This pattern matches **any** path segment named `data`, which caused Tailwind to skip:

- `src/components/data/` — DataConnectPage, DataManagePage, OnboardingChatPanel, etc.
- `src/data/` — dummy widget data

Classes used *only* in those files were never generated. Classes that also appeared in
non-ignored files (e.g. `gap-3` used in `TemplatePicker.tsx`) worked fine — making the
issue look like a selective/random breakage rather than a scanning problem.

## Fix

Added `@source` directives to `src/index.css` — the official Tailwind v4 mechanism
for explicitly registering paths that `.gitignore` would otherwise exclude:

```css
@import "tailwindcss";

@source "../src/components/data";
@source "../src/data";
```

## How to Diagnose in Future

1. **Build the project** and inspect the CSS output file in `dist/assets/`.
2. **Search for a missing class** in the built CSS (e.g. `grid-cols-2`).
3. If it's absent, check which **source file** uses it.
4. Run `git check-ignore -v <path>` from the repo root to see if any `.gitignore` rule matches.
5. Add a `@source` directive in `src/index.css` for the affected directory.

## References

- [Tailwind v4 — Detecting classes in source files](https://tailwindcss.com/docs/detecting-classes-in-source-files)
- [Tailwind v4 — `@source` directive](https://tailwindcss.com/docs/functions-and-directives)
