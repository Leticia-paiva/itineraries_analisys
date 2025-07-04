itineraries_duckdb_transformed_mv = """CREATE VIEW {project}.{dw}.itineraries_duckdb_transformed_mv
AS
SELECT
  legId,
  -- All other columns are selected. String columns (except legId) are transformed to arrays to deal with || in the csv.
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
  SPLIT(segmentsDepartureTimeEpochSeconds, '||') AS segmentsDepartureTimeEpochSeconds_array,
  SPLIT(segmentsDepartureTimeRaw, '||') AS segmentsDepartureTimeRaw_array,
  SPLIT(segmentsArrivalTimeEpochSeconds, '||') AS segmentsArrivalTimeEpochSeconds_array,
  SPLIT(segmentsArrivalTimeRaw, '||') AS segmentsArrivalTimeRaw_array,
  SPLIT(segmentsArrivalAirportCode, '||') AS segmentsArrivalAirportCode_array,
  SPLIT(segmentsDepartureAirportCode, '||') AS segmentsDepartureAirportCode_array,
  SPLIT(segmentsAirlineName, '||') AS segmentsAirlineName_array,
  SPLIT(segmentsAirlineCode, '||') AS segmentsAirlineCode_array,
  SPLIT(segmentsEquipmentDescription, '||') AS segmentsEquipmentDescription_array,
  SPLIT(segmentsDurationInSeconds, '||') AS segmentsDurationInSeconds_array,
  SPLIT(segmentsDistance, '||') AS segmentsDistance_array,
  SPLIT(segmentsCabinCode, '||') AS segmentsCabinCode_array
FROM
  {project}.{dw}.itineraries_duckdb;"""