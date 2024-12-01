-- CREATE TABLE one (input VARCHAR);
-- \copy one from '01/input.txt'

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
SELECT 
    SUM(ABS(a.a - b.b))
FROM a
JOIN b ON a.rowid = b.rowid;
