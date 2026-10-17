# Search API facts (POST https://adilet.zan.kz/api/search), verified 2026-10-17

- page_size=100. For `rus` without filters: total_hits=234775. The `total_pages` field is inconsistent (11739 / 2348 in different replies).
- Repeating page 1 returns identical order. `aggregations_enabled=false` keeps the order and the count. `facets` is always empty. Pages 1 and 2 don't overlap.
- **Depth limit:** `page x page_size <= 10000` (HTTP 422 `PageWindowExceeded`, "narrow the query with filters"), and page 100 already returns 0 results. Plain paging reaches ~9,900 records, so the catalog must be split into slices.
- The `filters.date_from` / `date_to` filter (YYYY-MM-DD, also accepts ISO timestamps) actually works on **start_date**, not on created_at.
  The range 0001-01-01..2999-12-31 gives 234775 = the full total, so there are no undated records. There are no records before 1900, and 3 records dated 2027 or later.
  Plan: split ranges recursively until each slice has total_hits <= 9900, then check that sum(slices) == total_hits and that the unique-ID count matches.
- The `filters.status` filter works (string or list): err = 701. Client-code statuses: new, upd, err, yts, stp. Don't use it for the full crawl (all statuses are needed).
- Client-code sorts: relevance, hierarchy, start_date_asc/desc, adoption_date_asc/desc, publication_date_*, justice_reg_date_*, end_date_*, date_asc/desc.
- In search results `has_content` is false for every record, so it can't be used to skip documents (check through by-ngr).
- Kazakh: language=`kaz` (confirmed earlier through by-ngr).
- Occasional connection resets during the TLS handshake; retries with backoff help.

Not done yet: backup + new crawl state (source=api_search, lang, filters, sort, size, slice); slicing by start_date;
parallel page and document queues (2+2, one shared rps); corpus by completeness for any status (re-convert `review` docs);
stats by language/status/completeness; tests; README.
