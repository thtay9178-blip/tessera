# Missing Features & Issues Summary

Based on GitHub issues and codebase analysis, here's what's missing from Tessera.

## 🔴 Critical Security & Performance Issues

### 1. Authorization Gaps (Issue #73) - HIGH PRIORITY
**Status:** Missing
**Impact:** Security vulnerability - unauthorized access to proposals and other resources

**Missing:**
- Authorization checks on `GET /api/v1/proposals/{id}` - anyone can view proposals
- Authorization checks on `GET /api/v1/proposals` - no auth required
- Team ownership verification for write operations
- Consumer access verification for read operations

**Files Affected:**
- `src/tessera/api/proposals.py` - Missing auth dependencies
- `src/tessera/api/assets.py` - Need to verify auth checks
- `src/tessera/api/contracts.py` - Need to verify auth checks
- `src/tessera/api/registrations.py` - Need to verify auth checks

### 2. Rate Limiting (Issue #72) - HIGH PRIORITY
**Status:** Missing
**Impact:** Vulnerable to brute force, DDoS, resource exhaustion

**Missing:**
- Rate limiting middleware
- Per-endpoint rate limits
- Per-API-key rate limiting (preferred over IP)
- 429 responses with Retry-After headers

**Solution:** Implement using `slowapi` library

### 3. Caching Implementation (Issue #74) - HIGH PRIORITY
**Status:** Cache service exists but NOT USED
**Impact:** All reads hit database, poor scalability

**Missing:**
- Cache integration in contract endpoints
- Cache integration in asset endpoints
- Cache integration in schema diff operations
- Cache invalidation on updates

**Files:**
- `src/tessera/services/cache.py` - EXISTS but unused
- `src/tessera/api/contracts.py` - Need to add caching
- `src/tessera/api/assets.py` - Need to add caching
- `src/tessera/services/schema_diff.py` - Need to add caching

### 4. Input Validation & Sanitization (Issue #77) - HIGH PRIORITY
**Status:** Partial (Pydantic validates structure, but missing format validation)
**Impact:** Security risk from invalid inputs, DoS from large schemas

**Missing:**
- FQN format validation (should match pattern like `database.schema.table`)
- Version string validation (should validate semver format)
- Schema size limits (prevent DoS from large schemas)
- String length limits enforcement

**Files to Update:**
- `src/tessera/models/asset.py` - Add FQN validator
- `src/tessera/models/contract.py` - Add version and schema size validators
- `src/tessera/models/team.py` - Add name length validator

### 5. CORS Configuration (Issue #76) - HIGH PRIORITY
**Status:** Missing production restrictions
**Impact:** Security risk in production

**Missing:**
- Production-specific CORS restrictions
- Environment-based CORS configuration

## 🟡 Missing API Endpoints

### 6. List Registrations Endpoint - SPEC MISMATCH
**Status:** Missing
**Spec Says:** `GET /registrations` should list all registrations
**Current:** Only `GET /registrations/{id}` exists

**Missing:**
```python
@router.get("")
async def list_registrations(
    contract_id: UUID | None = Query(None),
    consumer_team_id: UUID | None = Query(None),
    status: RegistrationStatus | None = Query(None),
    params: PaginationParams = Depends(pagination_params),
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """List all registrations with filtering."""
```

**File:** `src/tessera/api/registrations.py`

### 7. Audit Trail Query API (Issue #90) - MEDIUM PRIORITY
**Status:** Missing
**Impact:** Can't query audit events - audit table exists but no endpoints

**Missing:**
- `GET /api/v1/audit/events` - List audit events with filters
- `GET /api/v1/audit/events/{event_id}` - Get specific event
- `GET /api/v1/audit/entities/{entity_type}/{entity_id}/history` - Entity history

**Files:**
- `src/tessera/api/audit.py` - NEW FILE NEEDED
- `src/tessera/db/models.py` - AuditEventDB exists

## 🟢 Missing Features

### 8. Recursive Impact Analysis (Issue #99) - HIGH PRIORITY
**Status:** Missing
**Impact:** Only identifies direct consumers, not downstream assets

**Current:** `POST /assets/{id}/impact` only shows direct consumers
**Needed:** Recursive traversal of `AssetDependencyDB` to show full blast radius

**Files:**
- `src/tessera/api/assets.py` - Update `analyze_impact` function
- Need to traverse dependencies recursively

### 9. Native Environment Support (Issue #98) - HIGH PRIORITY
**Status:** Missing
**Impact:** Users encode environment in FQN (e.g., `prod.db.table`), making tracking difficult

**Missing:**
- `environment` field on `AssetDB` or `ContractDB`
- Environment filtering in APIs
- Asset linking across environments for promotion tracking

**Files:**
- `src/tessera/db/models.py` - Add environment field
- `src/tessera/api/assets.py` - Add environment filtering
- Migration needed

### 10. Soft Deletes (Issue #100) - MEDIUM PRIORITY
**Status:** Missing
**Impact:** Hard deletes lose audit trail

**Missing:**
- `deleted_at` timestamp field on `AssetDB` and `TeamDB`
- `is_active` boolean field
- Query filtering to exclude deleted entities by default

**Files:**
- `src/tessera/db/models.py` - Add soft delete fields
- `src/tessera/api/assets.py` - Update delete endpoint
- `src/tessera/api/teams.py` - Update delete endpoint (if exists)
- Migration needed

### 11. PUT vs PATCH for Teams - SPEC MISMATCH
**Status:** Uses PATCH instead of PUT
**Spec Says:** `PUT /teams/{id}`
**Current:** `PATCH /teams/{id}`

**File:** `src/tessera/api/teams.py`

## 📊 Summary by Priority

### 🔴 Critical (Security & Performance)
1. **Authorization Gaps** (#73) - Security vulnerability
2. **Rate Limiting** (#72) - DDoS protection
3. **Caching** (#74) - Performance
4. **Input Validation** (#77) - Security
5. **CORS Restrictions** (#76) - Security

### 🟡 High Priority Features
6. **Recursive Impact Analysis** (#99) - Core feature
7. **Environment Support** (#98) - Core feature
8. **List Registrations** - API completeness

### 🟢 Medium Priority
9. **Audit Trail API** (#90) - DX improvement
10. **Soft Deletes** (#100) - Compliance

### 📝 Spec Mismatches
- Missing `GET /registrations` endpoint
- Teams uses PATCH instead of PUT

## Next Steps

1. **Immediate (Security):**
   - Fix authorization gaps (#73)
   - Add rate limiting (#72)
   - Add input validation (#77)
   - Restrict CORS (#76)

2. **Performance:**
   - Implement caching (#74)

3. **Features:**
   - Add recursive impact analysis (#99)
   - Add environment support (#98)
   - Add list registrations endpoint
   - Add audit trail API (#90)

4. **Compliance:**
   - Add soft deletes (#100)

