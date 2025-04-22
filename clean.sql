START TRANSACTION;

-- CTE to extract and transform data from the raw JSON stored in the 'stocks' table
WITH candidate_rows AS (
    SELECT
        s.id, -- Keep the source ID to track which rows to delete
        s.raw_json ->> 'ticker' AS ticker_symbol, -- Extract ticker from top-level JSON
        to_timestamp(((result_element ->> 't')::BIGINT) / 1000.0) AS bar_time, -- Convert ms timestamp
        (result_element ->> 'o')::NUMERIC AS open_val,
        (result_element ->> 'h')::NUMERIC AS high_val,
        (result_element ->> 'l')::NUMERIC AS low_val,
        (result_element ->> 'c')::NUMERIC AS close_val,
        (result_element ->> 'v')::BIGINT AS volume_val,
        (result_element ->> 'vw')::NUMERIC AS vwap_val,
        (result_element ->> 'n')::INTEGER AS trade_count_val
    FROM
        stocks s, -- Assuming your source table is named 'stocks' and has columns 'id' and 'raw_json' (jsonb)
        jsonb_array_elements(s.raw_json -> 'results') AS result_element -- Unnest the results array
    WHERE
        jsonb_typeof(s.raw_json -> 'results') = 'array' -- Ensure 'results' is an array
        AND s.raw_json ->> 'ticker' IS NOT NULL -- Ensure ticker exists
        AND s.raw_json -> 'results' IS NOT NULL -- Ensure results array exists
        AND jsonb_array_length(s.raw_json -> 'results') > 0 -- Process only if results array is not empty
)
-- Insert the extracted data into the target table 'stock_aggregates_5min'
INSERT INTO stock_aggregates_5min (
    ticker,
    bar_timestamp,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    vwap,         -- Matches the column name defined in CREATE TABLE
    trade_count   -- Matches the column name defined in CREATE TABLE
)
SELECT
    ticker_symbol,
    bar_time,
    open_val,
    high_val,
    low_val,
    close_val,
    volume_val,
    vwap_val,
    trade_count_val
FROM
    candidate_rows
-- If a bar for the same ticker and timestamp already exists, do nothing (skip insert)
ON CONFLICT (ticker, bar_timestamp) DO NOTHING;

-- Delete the rows from the source 'stocks' table whose results were successfully processed
-- This uses the IDs collected in the CTE
DELETE FROM stocks s
WHERE s.id IN (SELECT DISTINCT id FROM candidate_rows);

COMMIT;
