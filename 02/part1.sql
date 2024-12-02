-- CREATE TABLE two (input VARCHAR);
-- \copy two from '02/input.txt'

WITH arrays AS (
    -- Make the rows into arrays. Attach a report id
    SELECT
        regexp_split_to_array(input, ' ') AS report_values,
        ROW_NUMBER() OVER () AS report_id
    FROM two
),
unnested AS (
    -- Unnest the arrays into rows - retains the ordering.
    -- Checking the initial two values for direction
    SELECT
        report_id,
        UNNEST(report_values)::INTEGER AS report_value,
        CASE 
            WHEN report_values[1]::INTEGER > report_values[2]::INTEGER THEN 'desc'
            ELSE 'asc'
        END AS direction
    FROM arrays
),
lags AS (
    -- Getting lag values for distance comparison
    SELECT 
        report_id,
        report_value,
        LAG(report_value, 1) OVER (PARTITION BY report_id) AS report_value_lag,
        direction
    FROM unnested
),
measures AS (
    -- Flagging direction mismatches and distances
    SELECT
        report_id,
        report_value,
        report_value_lag,
        CASE 
            WHEN direction = 'asc' AND report_value <= report_value_lag
            THEN 0
            WHEN direction = 'desc' AND report_value >= report_value_lag
            THEN 0
        ELSE 1 END AS correct_direction,
        ABS(report_value - report_value_lag) AS distance
    FROM lags
),
sums AS (
    -- Aggregating the final values for the final check
    SELECT 
        report_id,
        AVG(correct_direction) IN (1,0) AS accepted_direction,
        MAX(distance) IN (1,2,3) AS accepted_distance
    FROM measures
    GROUP BY report_id
)
SELECT 
    COUNT(*)
FROM sums
WHERE accepted_direction IS TRUE AND accepted_distance IS TRUE

