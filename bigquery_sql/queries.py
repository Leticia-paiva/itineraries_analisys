check_keys_and_duplicated_rows = """
    SELECT
    legId,
    FORMAT_DATE('%Y%m%d', SAFE.PARSE_DATE('%Y-%m-%d', searchDate)) AS formatted_search_date,
    FORMAT_DATE('%Y%m%d', SAFE.PARSE_DATE('%Y-%m-%d', flightDate)) AS formatted_flight_date,
    startingAirport,
    destinationAirport,
    COUNT(*) AS duplicate_count
    FROM (select distinct * from dbt-project-459000.dw_itineraries.itineraries_raw)
    WHERE
        legId IS NOT NULL
        AND FORMAT_DATE('%Y%m%d', SAFE.PARSE_DATE('%Y-%m-%d', searchDate)) IS NOT NULL
        AND FORMAT_DATE('%Y%m%d', SAFE.PARSE_DATE('%Y-%m-%d', flightDate)) IS NOT NULL
        AND startingAirport IS NOT NULL
        AND destinationAirport IS NOT NULL
    GROUP BY
        legId,
        formatted_search_date,
        formatted_flight_date,
        startingAirport,
        destinationAirport
    HAVING
        COUNT(*) > 1
    ORDER BY
        duplicate_count DESC, legId, formatted_search_date, formatted_flight_date, startingAirport, destinationAirport
"""

itineraries_fact_table = """
CREATE OR REPLACE TABLE dbt-project-459000.dw_itineraries.itineraries_fact_table AS(
  WITH distinct_table as (
    select distinct * from dbt-project-459000.dw_itineraries.itineraries_raw
    )
  SELECT
    legId,
    SAFE.PARSE_DATE('%Y-%m-%d', searchDate) AS searchDate, 
    SAFE.PARSE_DATE('%Y-%m-%d', flightDate) AS flightDate, 
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
    (ROW_NUMBER() OVER (
      PARTITION BY
        flightDate,
        startingAirport,
        destinationAirport,
        legId
      ORDER BY
        searchDate DESC
    ) = 1) AS is_current,
    FORMAT('%s_%s_%s_%s_%s',
        legId,
        FORMAT_DATE('%Y%m%d', SAFE.PARSE_DATE('%Y-%m-%d', searchDate)),
        FORMAT_DATE('%Y%m%d', SAFE.PARSE_DATE('%Y-%m-%d', flightDate)),
        startingAirport,
        destinationAirport
    ) AS itinerary_sk
    FROM
      distinct_table 
    WHERE 
      legId IS NOT NULL
      AND SAFE.PARSE_DATE('%Y-%m-%d', searchDate) IS NOT NULL
      AND SAFE.PARSE_DATE('%Y-%m-%d', flightDate) IS NOT NULL
      AND startingAirport IS NOT NULL
      AND destinationAirport IS NOT NULL
      AND segmentsDepartureTimeRaw IS NOT NULL
      AND segmentsArrivalTimeRaw IS NOT NULL
      AND segmentsArrivalAirportCode IS NOT NULL
      AND segmentsDepartureAirportCode IS NOT NULL
      AND totalFare IS NOT NULL
      AND isNonStop IS NOT NULL
      AND isBasicEconomy IS NOT NULL
      AND seatsRemaining IS NOT NULL
  );
"""

itinerary_dimension_segments ="""
CREATE OR REPLACE TABLE dbt-project-459000.dw_itineraries.itinerary_dimension_segments AS
WITH distinct_table as (
select distinct * from dbt-project-459000.dw_itineraries.itineraries_raw
)
SELECT
  -- Surrogate key for the parent itinerary
  FORMAT('%s_%s_%s_%s_%s',
      t.legId,
      FORMAT_DATE('%Y%m%d', SAFE.PARSE_DATE('%Y-%m-%d', t.searchDate)),
      FORMAT_DATE('%Y%m%d', SAFE.PARSE_DATE('%Y-%m-%d', t.flightDate)),
      t.startingAirport,
      t.destinationAirport
  ) AS itinerary_sk,
  FORMAT('%s_%s_%s_%s_%s_%s',
    t.legId,
    FORMAT_DATE('%Y%m%d', SAFE.PARSE_DATE('%Y-%m-%d', t.searchDate)),
    FORMAT_DATE('%Y%m%d', SAFE.PARSE_DATE('%Y-%m-%d', t.flightDate)),
    t.startingAirport,
    t.destinationAirport,
    CAST(idx AS STRING)
  ) AS segment_sk,
  t.legId,
  SAFE.PARSE_DATE('%Y-%m-%d', t.searchDate) AS searchDate, 
  SAFE.PARSE_DATE('%Y-%m-%d', t.flightDate) AS flightDate, 
  t.startingAirport,
  t.destinationAirport,
  idx AS segment_index,
  segmentsDepartureTimeEpochSeconds[OFFSET(idx)] AS segmentDepartureTimeEpochSeconds,
  segmentsDepartureTimeRaw[OFFSET(idx)] AS segmentDepartureTimeRaw,
  segmentsArrivalTimeEpochSeconds[OFFSET(idx)] AS segmentArrivalTimeEpochSeconds,
  segmentsArrivalTimeRaw[OFFSET(idx)] AS segmentArrivalTimeRaw,
  segmentsArrivalAirportCode[OFFSET(idx)] AS segmentArrivalAirportCode,
  segmentsDepartureAirportCode[OFFSET(idx)] AS segmentDepartureAirportCode,
  segmentsAirlineName[OFFSET(idx)] AS segmentAirlineName,
  segmentsAirlineCode[OFFSET(idx)] AS segmentAirlineCode,
  segmentsEquipmentDescription[OFFSET(idx)] AS segmentEquipmentDescription,
  segmentsDurationInSeconds[OFFSET(idx)] AS segmentDurationInSeconds,
  segmentsDistance[SAFE_OFFSET(idx)] AS segmentDistance,
  segmentsCabinCode[OFFSET(idx)] AS segmentCabinCode
FROM (
  SELECT
    legId,
    searchDate,
    flightDate,
    startingAirport,
    destinationAirport,
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
  FROM distinct_table
  WHERE
    legId IS NOT NULL
    AND searchDate IS NOT NULL
    AND flightDate IS NOT NULL
    AND startingAirport IS NOT NULL
    AND destinationAirport IS NOT NULL
    AND segmentsDepartureTimeRaw IS NOT NULL
    AND segmentsArrivalTimeRaw IS NOT NULL
    AND segmentsArrivalAirportCode IS NOT NULL
    AND segmentsDepartureAirportCode IS NOT NULL
    -- Removed filters on non-array, non-key columns as they are no longer selected in the inner query
) AS t,
UNNEST(t.segmentsDepartureTimeEpochSeconds) WITH OFFSET AS idx;
"""

view_prices_analisys = """
CREATE OR REPLACE VIEW dbt-project-459000.dw_itineraries.price_analysis AS(
WITH ranked_and_compared_data AS (
    SELECT
      itinerary_sk,
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
      is_current,
      FIRST_VALUE(totalFare) OVER (
        PARTITION BY legId,
                     flightDate,
                     startingAirport,
                     destinationAirport
        ORDER BY searchDate ASC
      ) AS oldest_total_fare,
      LAG(totalFare, 1) OVER (
        PARTITION BY legId,
                     flightDate,
                     startingAirport,
                     destinationAirport
        ORDER BY searchDate ASC
      ) AS previous_total_fare
    FROM
       dbt-project-459000.dw_itineraries.itineraries_fact_table
  ),
  joined_and_aggregated_data AS (
    SELECT
      rcd.itinerary_sk,
      rcd.legId,
      rcd.is_current,
      rcd.searchDate,
      rcd.flightDate,
      rcd.startingAirport,
      rcd.destinationAirport,
      rcd.isBasicEconomy,
      rcd.isRefundable,
      rcd.isNonStop,
      rcd.seatsRemaining,
      rcd.totalFare,
      rcd.oldest_total_fare,
      rcd.previous_total_fare,
      ARRAY_AGG(
          STRUCT(
              seg.segmentDepartureTimeRaw,
              seg.segmentArrivalTimeRaw,
              seg.segmentArrivalAirportCode,
              seg.segmentDepartureAirportCode
          ) ORDER BY seg.segment_index ASC 
      ) AS flight_segments_details 
    FROM
      ranked_and_compared_data AS rcd
    LEFT JOIN
      dbt-project-459000.dw_itineraries.itinerary_dimension_segments AS seg
    ON
      rcd.itinerary_sk = seg.itinerary_sk
    GROUP BY
      rcd.itinerary_sk, 
      rcd.legId,
      rcd.is_current,
      rcd.searchDate,
      rcd.flightDate,
      rcd.startingAirport,
      rcd.destinationAirport,
      rcd.isBasicEconomy,
      rcd.isRefundable,
      rcd.isNonStop,
      rcd.seatsRemaining,
      rcd.totalFare,
      rcd.oldest_total_fare,
      rcd.previous_total_fare
  )
  SELECT
    itinerary_sk,
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
    flight_segments_details,
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
    joined_and_aggregated_data
  ORDER BY
    searchDate DESC,
    flightDate DESC,
    startingAirport,
    destinationAirport,
    legId
);
"""
view_flights_type_analisys = """
 CREATE OR REPLACE VIEW dbt-project-459000.dw_itineraries.flights_type_analisys AS (
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
     dbt-project-459000.dw_itineraries.itineraries_fact_table
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

