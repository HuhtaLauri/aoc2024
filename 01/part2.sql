WITH a AS (
    SELECT 
        SUBSTRING(input, 1, 5)::INTEGER AS a,
        ROW_NUMBER() OVER (ORDER BY SUBSTRING(input, 1, 5)::INTEGER) AS rowid
    FROM one
),
b AS (
    SELECT 
        SUBSTRING(input, 9, 13)::INTEGER AS b,
        ROW_NUMBER() OVER (ORDER BY SUBSTRING(input, 9, 13)::INTEGER) AS rowid
    FROM one
)
SELECT SUM(a.a * bb.count)
FROM a
JOIN (
    SELECT b.b, COUNT(*) AS count
    FROM b
    GROUP BY b.b
) bb ON a.a = bb.b
