# Update Script Maintenance Report

Date: 2026-03-03

- Ran updater via `make`.
- Fixed fetch source in `scripts/process.py` from `http` to `https` and added `raise_for_status()`.
- Added workflow token write permission in `.github/workflows/actions.yml` (`permissions: contents: write`).
- Regenerated data outputs (`archive/data.csv`, `data/airport-codes.csv`, `datapackage.json`).
