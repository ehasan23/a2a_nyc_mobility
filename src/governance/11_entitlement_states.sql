-- src/governance/11_entitlement_states.sql — Task 5.2: the same query in three entitlement states.
-- gold_platform_daily carries WITH ROW FILTER rf_scope ON (platform_code) and MASK mask_money on its
-- two driver-pay columns (declared in gold.sql). Both functions look up session_user() in
-- ops.entitlements, so ONE account can play three roles by editing its own row.
-- Screenshot each result (evidence E15). Run each block, then the query.

-- Who am I, exactly? The row must match this string character for character.
SELECT session_user() AS me;
SELECT * FROM workspace.prd_ops.entitlements;

-- The query ----------------------------------------------------------------------------------
SELECT platform_code, count(*) AS days, sum(trips) AS trips, sum(total_driver_pay) AS driver_pay
FROM workspace.prd_gold.gold_platform_daily GROUP BY ALL ORDER BY platform_code;

-- State 1: full access (the setup job gave you '*' and true). Expect: every platform, real driver pay.

-- State 2: a Lyft analyst without access to financial measures ---------------------------------
UPDATE workspace.prd_ops.entitlements SET scope_value = 'HV0005', can_see_sensitive = false
WHERE user_email = session_user();
-- re-run the query: only HV0005 rows, driver_pay is NULL

-- State 3: back to full access ----------------------------------------------------------------
UPDATE workspace.prd_ops.entitlements SET scope_value = '*', can_see_sensitive = true
WHERE user_email = session_user();
-- re-run the query: identical to state 1

-- Optional, if you invited a teammate: give them state 2 and ask them to run the query.
-- INSERT INTO workspace.prd_ops.entitlements VALUES ('teammate@bjitgroup.com', 'HV0005', false);

-- How to show the rules are attached (not just "trust me"):
DESCRIBE TABLE EXTENDED workspace.prd_gold.gold_platform_daily;       -- look for Row Filter and Column Mask rows
