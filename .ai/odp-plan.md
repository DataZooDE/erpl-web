# Implementation Plan: OData-ODP Replication for ERPL

## Overview

Target a reliable, idempotent, delta-capable ingestion from SAP ODP via OData v2 into DuckDB. Follow the agent-agnostic TDD loop in `.ai/development-workflow.md` using `make test_debug` (SQL) and the C++ test binary where applicable. Always update the checklist, refer to `.ai/development-workflow.md` for details on the working style. If function, infrastructure, build and tests are all in place, mark the item as completed.

## Implementation Checklist

### **Phase 1 — Service Registration & Metadata Discovery**

#### Core Functionality
- [ ] **ODP Functions Registration**: Create a file `erpl_odata_odp_functions.hpp` and `erpl_odata_odp_functions.cpp` that contains the ODP functions. Look at the datasphere implementation how to do this.
- [ ] **SAP OData Function Discovery**: Add a table valud functions `erpl_sap_odata_show` which uses the SAP service catalog enpoint to discover OData Services (V2 and V4).
- [ ] **ODP Service Discovery**: Add a table valued function which returns the ODP services `erpl_odp_odata_show`

#### Implementation Requirements
- [ ] **Resuse existing OData functions**: The implementation should reuse existing OData functions, Secrets, etc. for the HTTP client, OData parsing and content handling.
- [ ] **Error Handling**: The OData functions should handle errors gracefully and return appropriate error messages.

#### Tests Needed
- [ ] **SQL Tests**: Table creation, data ingestion, idempotent behavior
- [ ] **Error Tests**: Authentication failures, preference not applied, network errors

## Risks & Mitigations

- **Token staleness**: surface clear remediation and provide `resubscribe`.
- **Parallelism hazards**: enforce single-run per source via advisory locks or state flags.
- **Schema drift**: detect and require an explicit migration path before applying deltas.
- **Delta capability missing**: graceful fallback to full extraction with clear user notification.
- **Subscription conflicts**: detect and resolve through proper cleanup and re-establishment.
- **URL construction errors**: validate delta token format (`!deltatoken=` not `?deltatoken=`)
- **JSON parsing failures**: handle OData v2 response wrapper `{"d": {...}}` correctly

## Quantitative Acceptance (per feature increment)

- Tests: 100% of new tests pass locally and in CI.
- Coverage: non-decreasing for touched modules; target ≥80% for new logic.
- Performance: reasonable end-to-end on representative pages; avoid O(n²) operations.
- **Performance benchmarks**:
  - Handle package sizes up to 15,000 rows efficiently
  - Process delta tokens with minimal latency
  - Support concurrent operations on different sources
