CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.flights` AS
SELECT *
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.flights_data`