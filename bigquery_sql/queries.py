itineraries_duckdb_transactional = """
CREATE OR REPLACE TABLE   {project}.{dw}.itineraries_transactional AS
(
  WITH table_with_splited_arrays AS(
    SELECT
      legId,
      -- Details columns of the flight (except legId) are transformed to arrays to deal with || in the csv.
      searchDate,
      flightDate,
      startingAirport,
      destinationAirport,
      fareBasisCode,
      travelDuration,
      elapsedDays,
      isBasicEconomy,
      isRefundable,
      isNonStop,
      baseFare,
      totalFare,
      seatsRemaining,
      totalTravelDistance,
      SPLIT(segmentsDepartureTimeEpochSeconds, '||') AS segmentsDepartureTimeEpochSeconds,
      SPLIT(segmentsDepartureTimeRaw, '||') AS segmentsDepartureTimeRaw,
      SPLIT(segmentsArrivalTimeEpochSeconds, '||') AS segmentsArrivalTimeEpochSeconds,
      SPLIT(segmentsArrivalTimeRaw, '||') AS segmentsArrivalTimeRaw,
      SPLIT(segmentsArrivalAirportCode, '||') AS segmentsArrivalAirportCode,
      SPLIT(segmentsDepartureAirportCode, '||') AS segmentsDepartureAirportCode,
      SPLIT(segmentsAirlineName, '||') AS segmentsAirlineName,
      SPLIT(segmentsAirlineCode, '||') AS segmentsAirlineCode,
      SPLIT(segmentsEquipmentDescription, '||') AS segmentsEquipmentDescription,
      SPLIT(segmentsDurationInSeconds, '||') AS segmentsDurationInSeconds,
      SPLIT(segmentsDistance, '||') AS segmentsDistance,
      SPLIT(segmentsCabinCode, '||') AS segmentsCabinCode
    FROM
      {project}.{dw}.itineraries_raw 
    WHERE 
      legId is not null
      AND searchDate is not null 
      AND startingAirport is not null
      AND destinationAirport is not null
      AND flightDate is not null 
      AND segmentsDepartureTimeRaw is not null
      AND segmentsArrivalTimeRaw is not null
      AND segmentsArrivalAirportCode is not null
      AND segmentsDepartureAirportCode is not null
  ),
  table_with_fixed_flights_struct as (
    SELECT
        legId,
        searchDate,
        flightDate,
        startingAirport,
        destinationAirport,
        fareBasisCode,
        travelDuration,
        elapsedDays,
        isBasicEconomy,
        isRefundable,
        isNonStop,
        baseFare,
        totalFare,
        seatsRemaining,
        totalTravelDistance,
        ARRAY(
          SELECT AS STRUCT
            segmentsArrivalAirportCode,
            segmentsDepartureAirportCode,
            segmentsDepartureTimeEpochSeconds,
            segmentsDepartureTimeRaw,
            segmentsArrivalTimeEpochSeconds,
            segmentsArrivalTimeRaw,
            segmentsAirlineName,
            segmentsAirlineCode,
            segmentsEquipmentDescription,
            segmentsDurationInSeconds,
            segmentsDistance,
            segmentsCabinCode
        FROM UNNEST(segmentsArrivalAirportCode) segmentsArrivalAirportCode WITH OFFSET 
        JOIN UNNEST(segmentsDepartureAirportCode) segmentsDepartureAirportCode WITH OFFSET USING(OFFSET)
        JOIN UNNEST(segmentsDepartureTimeEpochSeconds) segmentsDepartureTimeEpochSeconds WITH OFFSET USING(OFFSET)
        JOIN UNNEST(segmentsDepartureTimeRaw) segmentsDepartureTimeRaw WITH OFFSET USING(OFFSET)
        JOIN UNNEST(segmentsArrivalTimeEpochSeconds) segmentsArrivalTimeEpochSeconds WITH OFFSET USING(OFFSET)
        JOIN UNNEST(segmentsArrivalTimeRaw) segmentsArrivalTimeRaw WITH OFFSET USING(OFFSET)
        JOIN UNNEST(segmentsAirlineName) segmentsAirlineName WITH OFFSET USING(OFFSET)
        JOIN UNNEST(segmentsAirlineCode) segmentsAirlineCode WITH OFFSET USING(OFFSET)
        JOIN UNNEST(segmentsEquipmentDescription) segmentsEquipmentDescription WITH OFFSET USING(OFFSET)
        JOIN UNNEST(segmentsDurationInSeconds) segmentsDurationInSeconds WITH OFFSET USING(OFFSET)
        JOIN UNNEST(segmentsDistance) segmentsDistance WITH OFFSET USING(OFFSET)
        JOIN UNNEST(segmentsCabinCode) segmentsCabinCode WITH OFFSET USING(OFFSET)
      ) flight_details
    FROM table_with_splited_arrays
  )
  SELECT
      *,
      (ROW_NUMBER() OVER (
        PARTITION BY
          flightDate,
          startingAirport,
          destinationAirport,
          legId
        ORDER BY
          searchDate DESC
      ) = 1) AS is_current
  FROM
    table_with_fixed_flights_struct
  );
"""

view_prices_analisys = """
 CREATE OR REPLACE VIEW {project}.{dw}.price_analysis AS (
  WITH ranked_and_compared_data AS (
    SELECT
      *,
      FIRST_VALUE(totalFare) OVER (
        PARTITION BY flightDate, startingAirport, destinationAirport, legId
        ORDER BY searchDate ASC
      ) AS oldest_total_fare,
      LAG(totalFare, 1) OVER (
        PARTITION BY flightDate, startingAirport, destinationAirport, legId
        ORDER BY searchDate ASC
      ) AS previous_total_fare
    FROM
      {project}.{dw}.itineraries_transactional
  )
  SELECT
    legId,
    is_current,
    searchDate,
    flightDate,
    startingAirport,
    destinationAirport,
    isBasicEconomy,
    isRefundable,
    isNonStop,
    seatsRemaining,
    totalFare,
    ARRAY(
          SELECT AS STRUCT
              segmentsDepartureTimeRaw,
              segmentsArrivalTimeRaw,
              segmentsArrivalAirportCode,
              segmentsDepartureAirportCode
          FROM
              UNNEST(flight_details) AS s
      ) AS flight_details,
    CASE
      WHEN totalFare > oldest_total_fare THEN TRUE
      ELSE FALSE
    END AS price_went_up_vs_oldest,
    CASE
      WHEN totalFare < oldest_total_fare THEN TRUE
      ELSE FALSE
    END AS price_went_down_vs_oldest,
    CASE
      WHEN previous_total_fare IS NULL THEN 'N/A'
      WHEN totalFare > previous_total_fare THEN 'HIGHER'
      WHEN totalFare < previous_total_fare THEN 'LOWER'
      ELSE 'SAME'
    END AS price_change_vs_previous,
    --price went down compared to the oldest searchDate AND has less than 10 seats remaining
    CASE
      WHEN totalFare < oldest_total_fare AND seatsRemaining < 10 THEN TRUE
      ELSE FALSE
    END AS price_down_and_low_seats_vs_oldest
  FROM
    ranked_and_compared_data
  ORDER BY 
    searchDate DESC,
    flightDate DESC,
    startingAirport,
    destinationAirport,
    legId);
"""
view_flights_type_analisys = """
 CREATE OR REPLACE VIEW {project}.{dw}.flights_type_analisys AS (
    SELECT
      flightDate,
      startingAirport,
      destinationAirport,
      COUNT(CASE WHEN isNonStop THEN 1 END) AS non_stop_flights,
      COUNT(CASE WHEN NOT isNonStop THEN 1 END) AS not_non_stop_flights,
      COUNT(CASE WHEN isBasicEconomy THEN 1 END) AS basic_economy_flights,
      COUNT(CASE WHEN NOT isBasicEconomy THEN 1 END) AS not_basic_economy_flights,
      COUNT(CASE WHEN isNonStop and isBasicEconomy THEN 1 END) AS non_stop_economic_flights,
      COUNT(CASE WHEN NOT isNonStop and isBasicEconomy THEN 1 END) AS stop_economic_flights,
      COUNT(CASE WHEN isNonStop and NOT isBasicEconomy THEN 1 END) AS non_stop_not_economic_flights,
      COUNT(CASE WHEN NOT isNonStop and NOT isBasicEconomy THEN 1 END) AS stop_not_economic_flights,
      COUNT(*) AS total_flights_in_group
    FROM
     {project}.{dw}.itineraries_transactional
    WHERE is_current is true
    GROUP BY
      flightDate,
      startingAirport,
      destinationAirport
    ORDER BY
      flightDate,
      startingAirport,
      destinationAirport
  );
"""
