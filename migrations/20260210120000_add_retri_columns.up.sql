ALTER TABLE url_results
    ADD COLUMN car_files_percent NUMERIC(5, 2),
    ADD COLUMN large_files_percent NUMERIC(5, 2);

ALTER TABLE url_results
    ADD CONSTRAINT url_results_car_retri_check
    CHECK (car_files_percent IS NULL OR (car_files_percent >= 0.0 AND car_files_percent <= 100.0));

ALTER TABLE url_results
    ADD CONSTRAINT url_results_full_piece_retri_check
    CHECK (large_files_percent IS NULL OR (large_files_percent >= 0.0 AND large_files_percent <= 100.0));
