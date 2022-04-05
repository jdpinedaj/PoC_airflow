WITH flights_airlines AS (
    SELECT
        flights.year,
        flights.month,
        flights.day,
        flights.airline as airline_iata_code,
        airlines.airline,
        flights.flight_number,
        flights.origin_airport
    FROM `{{ params.project_id }}.{{ params.my_dataset }}.flights` flights
    LEFT JOIN `{{ params.project_id }}.{{ params.my_dataset }}.airlines` airlines
    ON flights.airline = airlines.iata_code
    )
SELECT 
        year,
        month,
        day,
        airline_iata_code,
        airline,
        flight_number,
        origin_airport,
        airports.airport AS name_airport,
        airports.city,
        airports.state,
        airports.latitude,
        airports.longitude
FROM flights_airlines
LEFT JOIN `{{ params.project_id }}.{{ params.my_dataset }}.airports` airports
ON flights_airlines.origin_airport = airports.iata
LIMIT 30