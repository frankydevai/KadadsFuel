# Kadads application workspace

This repository contains the shared Kadads bot and operations dashboard.

- Application package: `dieselup/`
- Dashboard package: `dieselup/dashboard/`
- Tests: `tests/`
- Do not add, copy, or modify BigRig (`bigrig/`) code in this workspace.
- Never deploy unless the user explicitly requests deployment.
- Keep credentials and private driver, truck, and price exports out of Git.
- Preserve the current scope of trucks 6682, 8089, and 8217 and disabled automatic linking unless the owner explicitly changes it.
- Run relevant tests before publishing changes. Preserve price and advice history; review additive database migrations separately.
