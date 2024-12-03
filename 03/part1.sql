-- CREATE TABLE three (input VARCHAR);
-- \copy three from '03/input.txt'

WITH numbers AS (
    SELECT
        REGEXP_SPLIT_TO_ARRAY(
        REGEXP_REPLACE(
        REGEXP_REPLACE(
            (REGEXP_MATCHES(input, 'mul\(\d+\,\d+\)', 'g'))[1],
            'mul\(', ''
        ),
        '\)', ''
        ),
        ','
        ) AS number_array
    FROM three
)
SELECT SUM(number_array[1]::INTEGER * number_array[2]::INTEGER)
FROM numbers
