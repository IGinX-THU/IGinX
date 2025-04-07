-- spotless:off
-- filter
SELECT *
FROM (SELECT val.bool AS b1,
             val.bool AS b2,
             num.i AS i1,
             num.i AS i2,
             num.l AS l1,
             num.l AS l2,
             num.f AS f1,
             num.f AS f2,
             num.d AS d1,
             num.d AS d2,
             val.bin AS v1,
             val.bin AS v2
      FROM (SELECT * FROM *))
WHERE b1 == b2 AND v1 >= v2 AND i1 <= i2
  AND (l1 <> l2 OR f1 < f2 OR d1 > d2 OR NOT ( v1 LIKE '^$' OR v2 NOT LIKE '^.*'))
  AND (KEY < 0 OR KEY >= 0)
  AND( v1 in ('0', '1', '2') OR v2 not in ('0', '1', '2'));

-- expression
SELECT
    iv + iv, iv - iv, iv * iv, iv / iv, iv % iv,
    iv + lv, iv - lv, iv * lv, iv / lv, iv % lv,
    iv + fv, iv - fv, iv * fv, iv / fv, iv % fv,
    iv + dv, iv - dv, iv * dv, iv / dv, iv % dv,
    lv + iv, lv - iv, lv * iv, lv / iv, lv % iv,
    lv + lv, lv - lv, lv * lv, lv / lv, lv % lv,
    lv + fv, lv - fv, lv * fv, lv / fv, lv % fv,
    lv + dv, lv - dv, lv * dv, lv / dv, lv % dv,
    fv + iv, fv - iv, fv * iv, fv / iv, fv % iv,
    fv + lv, fv - lv, fv * lv, fv / lv, fv % lv,
    fv + fv, fv - fv, fv * fv, fv / fv, fv % fv,
    fv + dv, fv - dv, fv * dv, fv / dv, fv % dv,
    dv + iv, dv - iv, dv * iv, dv / iv, dv % iv,
    dv + lv, dv - lv, dv * lv, dv / lv, dv % lv,
    dv + fv, dv - fv, dv * fv, dv / fv, dv % fv,
    dv + dv, dv - dv, dv * dv, dv / dv, dv % dv
FROM (
    SELECT num.i AS iv, num.l AS lv, num.f AS fv, num.d AS dv, val.bin AS vv
    FROM (SELECT * FROM * WHERE KEY > 0)
    );

SELECT CASE WHEN `l` > 100 THEN 1 ELSE 0 END FROM num;

SELECT
    EXTRACT(l,'year'),
    EXTRACT(l,'month'),
    EXTRACT(l,'day'),
    EXTRACT(l,'hour'),
    EXTRACT(l,'minute'),
    EXTRACT(l,'second')
FROM num;

SELECT SUBSTRING(bin, 1, 1) FROM val;

-- aggregation
SELECT COUNT(*), FIRST_VALUE(*), LAST_VALUE(*) FROM *;
SELECT COUNT(DISTINCT *) FROM *;
SELECT SUM(*), AVG(*), MIN(*), MAX(*) FROM num;
SELECT SUM(DISTINCT *), AVG(DISTINCT *), MIN(DISTINCT *), MAX(DISTINCT *) FROM num;

SELECT COUNT(*), FIRST_VALUE(*), LAST_VALUE(*) FROM (SELECT * FROM * WHERE key < 1000) GROUP BY num.i, num.l, num.f, num.d, val.bin, val.bool;
SELECT COUNT(DISTINCT *) FROM (SELECT * FROM * WHERE key < 1000) GROUP BY num.i, num.l, num.f, num.d, val.bin, val.bool;
SELECT SUM(*), AVG(*), MIN(*), MAX(*) FROM num WHERE key < 1000 GROUP BY i, l, f, d;
SELECT SUM(DISTINCT *), AVG(DISTINCT *), MIN(DISTINCT *), MAX(DISTINCT *) FROM num WHERE key < 1000 GROUP BY i, l, f, d;

SELECT COUNT(*), FIRST_VALUE(*), LAST_VALUE(*) FROM * OVER WINDOW (size 100 IN (0, 1000) SLIDE 50);
SELECT COUNT(DISTINCT *) FROM * OVER WINDOW (size 100 IN (0, 1000) SLIDE 50);
SELECT SUM(*), AVG(*), MIN(*), MAX(*) FROM num OVER WINDOW (size 100 IN (0, 1000) SLIDE 50);
SELECT SUM(DISTINCT *), AVG(DISTINCT *), MIN(DISTINCT *), MAX(DISTINCT *) FROM num OVER WINDOW (size 100 IN (0, 1000) SLIDE 50);

-- sort
SELECT * FROM * LIMIT 100 OFFSET 100;

SELECT * FROM * ORDER BY KEY ASC;
SELECT * FROM * ORDER BY KEY DESC;
SELECT * FROM * ORDER BY KEY ASC LIMIT 100;
SELECT * FROM * ORDER BY KEY DESC LIMIT 100;
SELECT * FROM * ORDER BY KEY ASC LIMIT 100 OFFSET 100;
SELECT * FROM * ORDER BY KEY DESC LIMIT 100 OFFSET 100;

SELECT * FROM num ORDER BY i ASC;
SELECT * FROM num ORDER BY i DESC;
SELECT * FROM num ORDER BY i ASC LIMIT 100;
SELECT * FROM num ORDER BY i DESC LIMIT 100;
SELECT * FROM num ORDER BY i ASC LIMIT 100 OFFSET 100;
SELECT * FROM num ORDER BY i DESC LIMIT 100 OFFSET 100;

SELECT * FROM num ORDER BY l ASC;
SELECT * FROM num ORDER BY l DESC;
SELECT * FROM num ORDER BY l ASC LIMIT 100;
SELECT * FROM num ORDER BY l DESC LIMIT 100;
SELECT * FROM num ORDER BY l ASC LIMIT 100 OFFSET 100;
SELECT * FROM num ORDER BY l DESC LIMIT 100 OFFSET 100;

SELECT * FROM num ORDER BY f ASC;
SELECT * FROM num ORDER BY f DESC;
SELECT * FROM num ORDER BY f ASC LIMIT 100;
SELECT * FROM num ORDER BY f DESC LIMIT 100;
SELECT * FROM num ORDER BY f ASC LIMIT 100 OFFSET 100;
SELECT * FROM num ORDER BY f DESC LIMIT 100 OFFSET 100;

SELECT * FROM num ORDER BY d ASC;
SELECT * FROM num ORDER BY d DESC;
SELECT * FROM num ORDER BY d ASC LIMIT 100;
SELECT * FROM num ORDER BY d DESC LIMIT 100;
SELECT * FROM num ORDER BY d ASC LIMIT 100 OFFSET 100;
SELECT * FROM num ORDER BY d DESC LIMIT 100 OFFSET 100;

SELECT * FROM val ORDER BY bool ASC;
SELECT * FROM val ORDER BY bool DESC;
SELECT * FROM val ORDER BY bool ASC LIMIT 100;
SELECT * FROM val ORDER BY bool DESC LIMIT 100;
SELECT * FROM val ORDER BY bool ASC LIMIT 100 OFFSET 100;
SELECT * FROM val ORDER BY bool DESC LIMIT 100 OFFSET 100;

SELECT * FROM val ORDER BY bin ASC;
SELECT * FROM val ORDER BY bin DESC;
SELECT * FROM val ORDER BY bin ASC LIMIT 100;
SELECT * FROM val ORDER BY bin DESC LIMIT 100;
SELECT * FROM val ORDER BY bin ASC LIMIT 100 OFFSET 100;
SELECT * FROM val ORDER BY bin DESC LIMIT 100 OFFSET 100;

SELECT * FROM val ORDER BY bool ASC, bin ASC;
SELECT * FROM val ORDER BY bool DESC, bin DESC;

-- join
WITH p(v) AS (SELECT l FROM num WHERE key < 100)
SELECT * FROM p AS l, p AS r;

WITH p(v) AS (SELECT l FROM num WHERE key < 100)
SELECT * FROM p AS x, p AS y where x.v = y.v AND x.v > 10;

SELECT * FROM num JOIN val USING KEY;

SELECT * FROM num AS x JOIN num AS y USING i JOIN num AS z ON x.i = z.i;
SELECT * FROM num AS x JOIN num AS y USING l JOIN num AS z ON x.l = z.l;
SELECT * FROM num AS x JOIN num AS y USING f JOIN num AS z ON x.f = z.f;
SELECT * FROM num AS x JOIN num AS y USING d JOIN num AS z ON x.d = z.d;
SELECT * FROM val AS x JOIN val AS y USING bin JOIN val AS z ON x.bin = z.bin;

SELECT * FROM num AS x LEFT JOIN num AS y USING i LEFT JOIN num AS z ON x.i = z.i;
SELECT * FROM num AS x LEFT JOIN num AS y USING l LEFT JOIN num AS z ON x.l = z.l;
SELECT * FROM num AS x LEFT JOIN num AS y USING f LEFT JOIN num AS z ON x.f = z.f;
SELECT * FROM num AS x LEFT JOIN num AS y USING d LEFT JOIN num AS z ON x.d = z.d;
SELECT * FROM val AS x LEFT JOIN val AS y USING bin LEFT JOIN val AS z ON x.bin = z.bin;

SELECT * FROM num AS x RIGHT JOIN num AS y USING i RIGHT JOIN num AS z ON y.i = z.i;
SELECT * FROM num AS x RIGHT JOIN num AS y USING l RIGHT JOIN num AS z ON y.l = z.l;
SELECT * FROM num AS x RIGHT JOIN num AS y USING f RIGHT JOIN num AS z ON y.f = z.f;
SELECT * FROM num AS x RIGHT JOIN num AS y USING d RIGHT JOIN num AS z ON y.d = z.d;
SELECT * FROM val AS x RIGHT JOIN val AS y USING bin RIGHT JOIN val AS z ON y.bin = z.bin;

SELECT * FROM num AS x FULL JOIN num AS y USING i FULL JOIN num AS z ON x.i = z.i;
SELECT * FROM num AS x FULL JOIN num AS y USING l FULL JOIN num AS z ON x.l = z.l;
SELECT * FROM num AS x FULL JOIN num AS y USING f FULL JOIN num AS z ON x.f = z.f;
SELECT * FROM num AS x FULL JOIN num AS y USING d FULL JOIN num AS z ON x.d = z.d;
SELECT * FROM val AS x FULL JOIN val AS y USING bin FULL JOIN val AS z ON x.bin = z.bin;

SELECT l-(SELECT AVG(`l`) FROM num) FROM num;
SELECT l FROM num WHERE EXISTS(SELECT * FROM num WHERE key =1);
SELECT l FROM num WHERE NOT EXISTS (SELECT * FROM num WHERE key =1);
SELECT l FROM num WHERE l IN (SELECT l FROM num);
SELECT l FROM num WHERE l NOT IN (SELECT l FROM num WHERE key =1);
SELECT l FROM num WHERE l > ALL (SELECT l FROM num WHERE key < 10);
SELECT l FROM num WHERE l > ANY (SELECT l FROM num WHERE key < 10);

-- set
SELECT DISTINCT * FROM *;
SELECT * FROM * UNION ALL SELECT * FROM *;
SELECT * FROM * UNION SELECT * FROM *;
SELECT * FROM * EXCEPT SELECT * FROM *;
SELECT * FROM * INTERSECT SELECT * FROM *;

-- not standard
SELECT FIRST(*), LAST(*) FROM *;

SELECT sequence() AS s1, sequence(-20, 30) AS s2 from *;

select KEY AS KEY from *;
SELECT KEY AS v, i AS KEY from num;
SELECT KEY AS v, l AS KEY from num;
SELECT KEY AS v, f AS KEY from num;
SELECT KEY AS v, d AS KEY from num;
SELECT KEY AS v, bin AS KEY from val;

SELECT * FROM (SHOW COLUMNS num.*);
SELECT value2meta(SELECT * FROM * LIMIT 10) FROM *;
-- spotless:on