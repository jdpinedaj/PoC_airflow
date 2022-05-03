WITH flights_airlines AS (
    SELECT flights.year,
        flights.month,
        flights.day,
        flights.flight_number,
        flights.origin_airport,
        flights.airline as airline_iata_code,
        airlines.airline
    FROM `{{ params.project_id }}.{{ params.my_dataset }}.flights_data` flights
        LEFT JOIN `{{ params.project_id }}.{{ params.my_dataset }}.airlines_data` airlines ON flights.airline = airlines.iata_code
)
SELECT year,
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
    INNER JOIN `{{ params.project_id }}.{{ params.my_dataset }}.airports_data` airports ON flights_airlines.origin_airport = airports.iata_code