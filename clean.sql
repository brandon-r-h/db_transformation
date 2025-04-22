START TRANSACTION;



WITH candidate_rows AS (
    SELECT
        s.id, 
        (result_element ->> 'T')::VARCHAR(10) AS ticker,
        to_timestamp( ((result_element ->> 't')::BIGINT) / 1000.0 ) AS bar_timestamp,
        (result_element ->> 'v')::NUMERIC::BIGINT AS volume,
        (result_element ->> 'vw')::NUMERIC(19, 4) AS volume_weighted_average_price,
        (result_element ->> 'o')::NUMERIC(19, 4) AS open_price,
        (result_element ->> 'c')::NUMERIC(19, 4) AS close_price,
        (result_element ->> 'h')::NUMERIC(19, 4) AS high_price,
        (result_element ->> 'l')::NUMERIC(19, 4) AS low_price,
        (result_element ->> 'n')::NUMERIC::BIGINT AS transaction_count
    FROM
        stocks s, 
        jsonb_array_elements(s.raw_json -> 'results') AS result_element
    WHERE
        jsonb_typeof(s.raw_json -> 'results') = 'array'
), insert_attempt AS (
    INSERT INTO stock_daily_bars (
        ticker, bar_timestamp, volume, volume_weighted_average_price,
        open_price, close_price, high_price, low_price, transaction_count
    )
    SELECT
        ticker, bar_timestamp, volume, volume_weighted_average_price,
        open_price, close_price, high_price, low_price, transaction_count
    FROM
        candidate_rows
    ON CONFLICT (ticker, bar_timestamp) DO NOTHING
)
DELETE FROM stocks s
WHERE s.id IN (SELECT DISTINCT id FROM candidate_rows);








COMMIT;
