ALTER TABLE url_results
    DROP CONSTRAINT IF EXISTS url_results_car_retri_check;

ALTER TABLE url_results
    DROP CONSTRAINT IF EXISTS url_results_full_piece_retri_check;

ALTER TABLE url_results
    DROP COLUMN IF EXISTS car_files_percent,
    DROP COLUMN IF EXISTS large_files_percent;
