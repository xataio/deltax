use pgrx::prelude::*;
use serde::{Deserialize, Serialize};

// ============================================================================
// first(value, timestamp) — returns the value at the earliest timestamp
// ============================================================================

/// State for the `first` aggregate.
#[derive(Copy, Clone, Default, Debug, PostgresType, Serialize, Deserialize)]
#[pg_binary_protocol]
pub struct FirstState {
    pub value: f64,
    pub ts: i64, // microseconds since PG epoch
    pub has_value: bool,
}

/// Name tag for the `first` aggregate.
#[derive(AggregateName)]
#[aggregate_name = "first"]
pub struct First;

#[pg_aggregate]
impl Aggregate<First> for First {
    const INITIAL_CONDITION: Option<&'static str> =
        Some(r#"{ "value": 0.0, "ts": 0, "has_value": false }"#);

    type State = FirstState;
    type Args = (f64, TimestampWithTimeZone);
    type Finalize = f64;

    fn state(
        mut current: Self::State,
        (value, ts): Self::Args,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::State {
        let ts_usec = ts.into_inner();
        if !current.has_value || ts_usec < current.ts {
            current.value = value;
            current.ts = ts_usec;
            current.has_value = true;
        }
        current
    }

    fn finalize(
        current: Self::State,
        _direct_args: Self::OrderedSetArgs,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::Finalize {
        current.value
    }
}

// ============================================================================
// last(value, timestamp) — returns the value at the latest timestamp
// ============================================================================

/// State for the `last` aggregate.
#[derive(Copy, Clone, Default, Debug, PostgresType, Serialize, Deserialize)]
#[pg_binary_protocol]
pub struct LastState {
    pub value: f64,
    pub ts: i64,
    pub has_value: bool,
}

/// Name tag for the `last` aggregate.
#[derive(AggregateName)]
#[aggregate_name = "last"]
pub struct Last;

#[pg_aggregate]
impl Aggregate<Last> for Last {
    const INITIAL_CONDITION: Option<&'static str> =
        Some(r#"{ "value": 0.0, "ts": 0, "has_value": false }"#);

    type State = LastState;
    type Args = (f64, TimestampWithTimeZone);
    type Finalize = f64;

    fn state(
        mut current: Self::State,
        (value, ts): Self::Args,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::State {
        let ts_usec = ts.into_inner();
        if !current.has_value || ts_usec > current.ts {
            current.value = value;
            current.ts = ts_usec;
            current.has_value = true;
        }
        current
    }

    fn finalize(
        current: Self::State,
        _direct_args: Self::OrderedSetArgs,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::Finalize {
        current.value
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_first_aggregate() {
        Spi::run(
            "CREATE TABLE test_fl (ts TIMESTAMPTZ, val FLOAT8);
             INSERT INTO test_fl VALUES
                ('2025-01-01 10:00:00+00', 1.0),
                ('2025-01-01 08:00:00+00', 2.0),
                ('2025-01-01 12:00:00+00', 3.0);",
        )
        .expect("setup failed");

        let result =
            Spi::get_one::<f64>("SELECT first(val, ts) FROM test_fl").expect("query failed");
        assert_eq!(
            result,
            Some(2.0),
            "first should return value at earliest time (08:00)"
        );
    }

    #[pg_test]
    fn test_last_aggregate() {
        Spi::run(
            "CREATE TABLE test_fl2 (ts TIMESTAMPTZ, val FLOAT8);
             INSERT INTO test_fl2 VALUES
                ('2025-01-01 10:00:00+00', 1.0),
                ('2025-01-01 08:00:00+00', 2.0),
                ('2025-01-01 12:00:00+00', 3.0);",
        )
        .expect("setup failed");

        let result =
            Spi::get_one::<f64>("SELECT last(val, ts) FROM test_fl2").expect("query failed");
        assert_eq!(
            result,
            Some(3.0),
            "last should return value at latest time (12:00)"
        );
    }
}
