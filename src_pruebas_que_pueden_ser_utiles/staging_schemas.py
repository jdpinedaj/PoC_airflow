from google.cloud.bigquery import SchemaField

tables = [
    'airlines',  # https://www.kaggle.com/datasets/usdot/flight-delays?select=airlines.csv
    'airports',  # https://www.kaggle.com/datasets/usdot/flight-delays?select=airports.csv
    'flights',  # https://www.kaggle.com/datasets/usdot/flight-delays?select=flights.csv
]

airlines = [
    SchemaField('iata_code', 'STRING', 'NULLABLE', None, ()),
    SchemaField('airline', 'STRING', 'NULLABLE', None, ()),
]

airports = [
    SchemaField('iata', 'STRING', 'NULLABLE', None, ()),
    SchemaField('airport', 'STRING', 'NULLABLE', None, ()),
    SchemaField('city', 'STRING', 'NULLABLE', None, ()),
    SchemaField('state', 'STRING', 'NULLABLE', None, ()),
    SchemaField('country', 'STRING', 'NULLABLE', None, ()),
    SchemaField('latitude', 'FLOAT', 'NULLABLE', None, ()),
    SchemaField('longitude', 'FLOAT', 'NULLABLE', None, ()),
]

flights = [
    SchemaField('year', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('month', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('day', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('day_of_week', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('airline', 'STRING', 'NULLABLE', None, ()),
    SchemaField('flight_number', 'STRING', 'NULLABLE', None, ()),
    SchemaField('tail_number', 'STRING', 'NULLABLE', None, ()),
    SchemaField('origin_airport', 'STRING', 'NULLABLE', None, ()),
    SchemaField('destination_airport', 'STRING', 'NULLABLE', None, ()),
    SchemaField('scheduled_departure', 'STRING', 'NULLABLE', None, ()),
    SchemaField('departure_time', 'STRING', 'NULLABLE', None, ()),
    SchemaField('departure_delay', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('taxi_out', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('wheels_off', 'STRING', 'NULLABLE', None, ()),
    SchemaField('scheduled_time', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('elapsed_time', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('air_time', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('distance', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('wheels_on', 'STRING', 'NULLABLE', None, ()),
    SchemaField('taxi_in', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('scheduled_arrival', 'STRING', 'NULLABLE', None, ()),
    SchemaField('arrival_time', 'STRING', 'NULLABLE', None, ()),
    SchemaField('arrival_delay', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('diverted', 'BOOLEAN', 'NULLABLE', None, ()),
    SchemaField('cancelled', 'BOOLEAN', 'NULLABLE', None, ()),
    SchemaField('cancellation_reason', 'STRING', 'NULLABLE', None, ()),
    SchemaField('air_system_delay', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('security_delay', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('airline_delay', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('late_aircraft_delay', 'INTEGER', 'NULLABLE', None, ()),
    SchemaField('weather_delay', 'INTEGER', 'NULLABLE', None, ()),
]
