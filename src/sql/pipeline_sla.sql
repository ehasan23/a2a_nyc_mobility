-- src/sql/pipeline_sla.sql — scheduled health check: fails if files landed more than 2 hours ago
-- and no release has been certified since (a stuck or silently failing pipeline).
-- If your warehouse rejects the :marker syntax, move this check into a notebook task and note it in the README.
SELECT assert_true(
         (SELECT count(*) FROM IDENTIFIER(:manifest_table)) = 0
      OR (SELECT max(registered_at) FROM IDENTIFIER(:manifest_table)) > current_timestamp() - INTERVAL 2 HOURS
      OR (SELECT coalesce(max(certified_at), TIMESTAMP'1970-01-01') FROM IDENTIFIER(:release_table))
           >= (SELECT max(registered_at) FROM IDENTIFIER(:manifest_table)),
         'SLA breach: files landed more than 2 hours ago have not been certified') AS pipeline_is_keeping_up;
