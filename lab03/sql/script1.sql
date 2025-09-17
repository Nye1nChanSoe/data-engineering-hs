-- zip 7 Nyein Chan Soe
-- 47833c2702e9fbde2c0ca55fea2c6283 

SELECT session_code
FROM sessions
WHERE session_name ILIKE '%GOLDEN WALRUS%'
limit 1
-- S3


DROP TABLE IF EXISTS badges;
CREATE TEMP TABLE badges AS
SELECT DISTINCT badge_uid
FROM swipes
WHERE session_code = 'S3'
  AND DATE(ts) = DATE '2025-09-01'
  AND ts::time >= TIME '13:00'
  AND ts::time <  TIME '13:15';

select * from badges

-- BDG-361873133
-- BDG-38012679
-- BDG-535555814
-- BDG-594015330
-- BDG-624326570
-- BDG-653507995
-- BDG-

DROP TABLE IF EXISTS cafe;
CREATE TEMP TABLE cafe AS
SELECT
  p.badge_uid,
  p.ts,
  p.product,
  (p.product ILIKE '%coffee%' OR p.product ILIKE '%espresso%') AS is_coffee
FROM purchases p
JOIN badges t USING (badge_uid)
WHERE DATE(p.ts) = DATE '2025-09-01'
  AND p.ts::time >= TIME '15:00'
  AND p.location ILIKE 'cafeteria'
ORDER BY p.badge_uid, p.ts;

-- BDG-38012679	2025-09-01 15:44:03.000	Tea	false
-- BDG-653507995	2025-09-01 15:37:32.000	Coffee	true
-- BDG-653507995	2025-09-01 15:55:48.000	Coffee	true
-- BDG-653507995	2025-09-01 16:50:51.000	Coffee	true
-- BDG-940715613	2025-09-01 15:27:10.000	Coffee	true
-- BDG-940715613	2025-09-01 15:48:51.000	Coffee	true
-- BDG-940715613	2025-09-01 16:41:34.000	Coffee	true


DROP TABLE IF EXISTS culprits;
CREATE TEMP TABLE culprits AS
SELECT badge_uid
FROM cafe
WHERE is_coffee
GROUP BY badge_uid
HAVING COUNT(*) >= 3;

-- BDG-653507995
-- BDG-940715613

SELECT
  p.full_name,
  c.badge_uid,
  md5(lower(trim(p.full_name))) AS proof_checksum
FROM culprits c
JOIN people p USING (badge_uid)
WHERE md5(lower(trim(p.full_name))) = '47833c2702e9fbde2c0ca55fea2c6283';
