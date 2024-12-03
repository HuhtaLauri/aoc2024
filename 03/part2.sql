WITH events AS (
    -- Grab the event stream from the input
    -- Valid events:
    --  mul(x,x)
    --  do()
    --  don't()
    SELECT
        (REGEXP_MATCHES(
            input, 'do\(\)|don''t\(\)|mul\(\d{1,3}\,\d{1,3}\)', 'g'
        ))[1] AS input
    FROM three
),
cleaning AS (
    -- Pick the numbers from mul() rows
    -- Assign rowid
 SELECT
    CASE 
      WHEN SUBSTRING(input, 1, 3) = 'mul'
    THEN
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            input, 'mul\(', ''
        ), '(?<=\d)\)', ''
    )
    ELSE input END AS input,
    ROW_NUMBER() OVER () AS rowid
 FROM events
),
start AS (
    -- Assign the start state giving valid = 1
    SELECT
        input,
        1 AS valid,
        rowid
    FROM cleaning
    WHERE rowid = 1
    UNION ALL
    SELECT 
        input,
        NULL AS valid,
        rowid
    FROM cleaning
    WHERE rowid > 1
),
arrays AS (
    -- Make the inputs into proper arrays
    -- Valid flag alternating based on events
    -- Do/Don't events turned into {0} arrays
    SELECT 
        CASE
          WHEN input = 'do()' OR input = 'don''t()' THEN ARRAY[0]
          ELSE REGEXP_SPLIT_TO_ARRAY(input, ',')::INTEGER[]
        END AS input,
        CASE 
          WHEN valid IS NOT NULL THEN valid
          WHEN input = 'don''t()' THEN 0
          WHEN input = 'do()' THEN 1
        END AS valid,
        rowid
    FROM start
),
repeats AS (
    -- Repeat the last valid state for each row
    -- Postgresql LAG() dont have flag to ignore nulls so we do this the hard way
    SELECT 
        input,
        valid AS valid_org,
        valid_partition,
        first_value(valid) over (partition by valid_partition order by rowid) AS valid,
        rowid
    FROM (
        SELECT 
            input,
            valid,
            sum(case when valid is null then 0 else 1 end) over (order by rowid) as valid_partition,
            rowid
        FROM arrays
        ORDER BY rowid ASC
    )
)
SELECT 
    -- Finally we are able to sum all rows that are valid
    SUM(input[1] * input[array_upper(input, 1)])
FROM repeats
WHERE valid = 1
