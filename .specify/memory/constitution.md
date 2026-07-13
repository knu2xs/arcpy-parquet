<!--
Sync Impact Report
- Version change: template -> 1.0.0
- Modified principles:
	- Template Principle 1 -> I. Python Quality and Documentation Discipline
	- Template Principle 2 -> II. Testable Data Conversion Behavior
	- Template Principle 3 -> III. ArcPy Performance and Resource Safety
	- Template Principle 4 -> IV. Reproducible Data Workflow and Packaging
	- Template Principle 5 -> V. Security and Configuration Hygiene
- Added sections:
	- Project Constraints
	- Workflow and Quality Gates
- Removed sections:
	- None
- Templates requiring updates:
	- ✅ d:\projects\arcpy-parquet\.specify\templates\plan-template.md
	- ✅ d:\projects\arcpy-parquet\.specify\templates\spec-template.md
	- ✅ d:\projects\arcpy-parquet\.specify\templates\tasks-template.md
	- ✅ d:\projects\arcpy-parquet\.github\prompts\speckit.constitution.prompt.md (validated, no change needed)
- Deferred TODOs:
	- None
-->

# ArcPy-Parquet Constitution

## Core Principles

### I. Python Quality and Documentation Discipline
All production Python code MUST comply with PEP8, include explicit type hints for
function and method arguments and return values, and include Google-style
docstrings with `Args:` and, where applicable, `Returns:` and `Raises:`. For
non-obvious implementation logic, maintainers MUST add concise explanatory
comments. This rule is non-negotiable because this repository is a shared data
engineering and GIS toolkit with long-lived maintenance needs.

### II. Testable Data Conversion Behavior
Any change that alters conversion behavior between feature classes, Parquet, or
GeoParquet MUST include automated tests in `testing/` that verify correctness of
schema mapping, value handling, and failure modes. New behavior MUST be
independently testable and MUST preserve compatibility expectations unless a
breaking change is explicitly approved and versioned. This rule exists to protect
data integrity and user trust in geospatial transformations.

### III. ArcPy Performance and Resource Safety
When implementing ArcPy data update workflows, contributors MUST prefer
`arcpy.da.UpdateCursor` for field updates and SHOULD use generators where practical
to reduce memory overhead. Multi-step geoprocessing pipelines SHOULD use ArcPy
`memory` workspace for intermediate outputs unless profiling or platform
constraints demonstrate a safer alternative. This rule reduces runtime cost and
avoids unnecessary disk I/O for large geospatial workloads.

### IV. Reproducible Data Workflow and Packaging
Changes that affect data ingestion, processing, packaging, or release workflows
MUST preserve reproducibility through documented make targets and scripts.
`make env`, `make data`, `make docs`, `make test`, and `make pytpkg` pathways MUST
remain usable or be updated in the same change set. Any tooling change MUST
update the relevant script or documentation in `README.md`, `scripts/`, `docsrc/`,
or `arcgis/README.md`. This rule ensures contributors can reliably reproduce
results and deliverables.

### V. Security and Configuration Hygiene
Secrets and credentials MUST be isolated to `config/secrets.ini` or environment
equivalents and MUST NOT be hard-coded in source, notebooks, tests, or scripts.
Configuration defaults MUST remain in tracked config files while sensitive values
stay out of version control. This rule limits accidental secret exposure and
supports secure collaboration.

## Project Constraints

- Primary implementation language is Python with ArcPy-centric geospatial
	processing, with pandas/numpy/scikit-learn preferred where appropriate.
- Repository structure MUST keep source in `src/`, tests in `testing/`, scripts in
	`scripts/`, notebooks in `notebooks/`, and configuration in `config/`.
- Feature work MUST preserve compatibility with documented project workflows and
	ArcGIS tooling assets under `arcgis/`.

## Workflow and Quality Gates

- Every feature spec and plan MUST include a Constitution Check that confirms
	compliance with all five core principles.
- Implementation plans MUST identify required tests, documentation updates, and
	security/configuration impacts before coding begins.
- Task lists MUST include explicit tasks for tests, docs or workflow updates, and
	secret/configuration validation when relevant.
- Pull requests MUST pass test execution and include evidence of updated docs when
	workflows or user-facing behavior change.

## Governance

This constitution supersedes conflicting local conventions for planning,
implementation, and review.

Amendment procedure:
1. Propose amendments in a pull request that includes rationale, impacted
	 templates, and migration implications.
2. Obtain approval from project maintainers.
3. Update this constitution and any impacted `.specify` templates in the same
	 change.

Versioning policy:
1. MAJOR version increments for incompatible governance changes or removal of a
	 core principle.
2. MINOR version increments for new principles, new mandatory sections, or
	 materially expanded governance requirements.
3. PATCH version increments for clarifications, wording improvements, and
	 non-semantic edits.

Compliance review expectations:
1. Every implementation plan and task set MUST be reviewed against this
	 constitution before execution.
2. Reviewers MUST flag violations and require either remediation or an explicit
	 justified exception in Complexity Tracking.
3. Runtime development guidance in `AGENTS.md` and repository docs MUST remain
	 aligned with this constitution.

**Version**: 1.0.0 | **Ratified**: 2026-03-08 | **Last Amended**: 2026-03-08
