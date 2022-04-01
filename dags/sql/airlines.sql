CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.airlines` AS
SELECT *
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.airlines_data`