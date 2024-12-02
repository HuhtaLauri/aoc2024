WITH arrays AS (
    -- Make the rows into arrays. Attach a report id
    SELECT
        regexp_split_to_array(input, ' ') AS report_values,
        ROW_NUMBER() OVER () AS report_id
    FROM two
),
groups AS (
    -- Multiply by array length and assign group id
    SELECT 
        report_id,
        ROW_NUMBER() OVER (PARTITION BY report_id) AS group_id,
        report_values 
    FROM 
        arrays,
        generate_series(1, array_length(report_values, 1)) AS seq
),
patch_arrays AS (
    -- Remove group_id index
    SELECT 
        report_id,
        group_id,
        report_values[:group_id::INTEGER-1] || report_values[group_id::INTEGER+1:] AS report_values
    FROM groups
),
directions AS (
    SELECT 
        report_id,
        group_id,
        UNNEST(report_values)::INTEGER AS report_value,
        CASE
            WHEN report_values[1]::INTEGER > report_values[2]::INTEGER THEN 'desc'
            ELSE 'asc' END AS direction
    FROM patch_arrays
),
distances AS (
    SELECT 
        *,
        LAG(report_value, 1) OVER (PARTITION BY report_id, group_id) AS report_value_lag,
        ABS(report_value - LAG(report_value, 1) OVER (PARTITION BY report_id, group_id)) AS distance
    FROM directions
),
measures AS (
    -- Flagging direction mismatches and distances
    SELECT
        *,
        CASE 
            WHEN direction = 'asc' AND report_value <= report_value_lag
            THEN 0
            WHEN direction = 'desc' AND report_value >= report_value_lag
            THEN 0
        ELSE 1 END AS correct_direction
    FROM distances
),
sums AS (
    -- Aggregating the final values for the final check
    SELECT 
        report_id,
        AVG(correct_direction) IN (1) AS accepted_direction,
        MAX(distance) IN (1,2,3) AS accepted_distance
    FROM measures
    GROUP BY report_id, group_id
),
accepted_reports AS (
    SELECT report_id,
            AVG(
                CASE WHEN accepted_distance IS TRUE AND accepted_direction IS TRUE THEN 1
                ELSE 0 END
            ) AS accepted_report
    FROM sums
    GROUP BY report_id
)
SELECT 
    COUNT(*)
FROM accepted_reports
WHERE accepted_report > 0;
