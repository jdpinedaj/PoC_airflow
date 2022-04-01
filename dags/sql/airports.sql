CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.airports` AS
SELECT *
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.airports_data`