view: noaa_storms {
  dimension: event_id { type: number }
  dimension: event_type { type: string }
  dimension: state { type: string }
  dimension: event_date { type: date }
  measure: total_storms { type: count }
  measure: total_injuries { type: sum, sql: ${injuries_direct} }
  measure: total_deaths { type: sum, sql: ${deaths_direct} }
}
