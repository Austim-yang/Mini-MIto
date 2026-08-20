use datafusion::{
    logical_expr::{BinaryExpr, Expr, Operator},
    scalar::ScalarValue,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeRange {
    pub min: Option<i64>,
    pub max: Option<i64>,
}

impl TimeRange {
    pub fn unbounded() -> Self {
        TimeRange {
            min: None,
            max: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!((self.min, self.max), (Some(low), Some(high)) if low > high)
    }

    pub fn to_inclusive_bounds(&self) -> Option<(i64, i64)> {
        if self.is_empty() {
            None
        } else {
            Some((self.min.unwrap_or(i64::MIN), self.max.unwrap_or(i64::MAX)))
        }
    }
}

pub fn extract_time_range(filters: &[Expr], time_col: &str) -> TimeRange {
    let mut range = TimeRange::unbounded();
    for f in filters {
        apply_predicate(&mut range, f, time_col);
    }
    range
}

fn apply_predicate(range: &mut TimeRange, expr: &Expr, time_col: &str) {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return;
    };
    let (column, literal, op) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) => (c, v, *op),
        (Expr::Literal(v, _state), Expr::Column(c)) => (c, v, swap_op(*op)),
        _ => return,
    };
    if column.name != time_col {
        return;
    }
    let Some(ts) = scalar_to_i64(literal) else {
        return;
    };
    apply_bound(range, op, ts);
}

fn swap_op(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

fn scalar_to_i64(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::Int32(Some(x)) => Some(*x as i64),
        ScalarValue::Int64(Some(x)) => Some(*x),
        ScalarValue::TimestampNanosecond(Some(x), None) => Some(*x),
        _ => None,
    }
}

fn apply_bound(range: &mut TimeRange, op: Operator, ts: i64) {
    match op {
        Operator::Gt | Operator::GtEq => {
            range.min = Some(range.min.map_or(ts, |m| m.max(ts)));
        }
        Operator::Lt | Operator::LtEq => {
            range.max = Some(range.max.map_or(ts, |m| m.min(ts)));
        }
        Operator::Eq => {
            range.min = Some(range.min.map_or(ts, |m| m.max(ts)));
            range.max = Some(range.max.map_or(ts, |m| m.min(ts)));
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit, utils::split_conjunction};

    fn ts(col_name: &str, op: Operator, v: i64) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col(col_name)),
            op,
            right: Box::new(lit(v)),
        })
    }

    #[test]
    fn test_extract_gt_lt_eq() {
        let r = extract_time_range(&[ts("timestamp", Operator::Gt, 100)], "timestamp");
        assert_eq!(
            r,
            TimeRange {
                min: Some(100),
                max: None
            }
        );

        let r = extract_time_range(&[ts("timestamp", Operator::Lt, 200)], "timestamp");
        assert_eq!(
            r,
            TimeRange {
                min: None,
                max: Some(200)
            }
        );

        let r = extract_time_range(&[ts("timestamp", Operator::Eq, 150)], "timestamp");
        assert_eq!(
            r,
            TimeRange {
                min: Some(150),
                max: Some(150)
            }
        );
    }

    #[test]
    fn test_extract_reversed_and_contradiction() {
        let reversed = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit(50)),
            op: Operator::Lt,
            right: Box::new(col("timestamp")),
        });
        let r = extract_time_range(&[reversed], "timestamp");
        assert_eq!(
            r,
            TimeRange {
                min: Some(50),
                max: None
            }
        );

        let exprs = split_conjunction(&Expr::and(
            ts("timestamp", Operator::Gt, 100),
            ts("timestamp", Operator::Lt, 50),
        ))
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
        let r = extract_time_range(&exprs, "timestamp");
        assert!(r.is_empty());
    }

    #[test]
    fn test_extract_ignores_non_ts() {
        let r = extract_time_range(&[ts("tags", Operator::Eq, 1)], "timestamp");
        assert_eq!(r, TimeRange::unbounded());
    }

    #[test]
    fn test_to_inclusive_bounds() {
        let r = TimeRange {
            min: Some(10),
            max: None,
        };
        assert_eq!(r.to_inclusive_bounds(), Some((10, i64::MAX)));
        let r = TimeRange {
            min: Some(100),
            max: Some(50),
        };
        assert_eq!(r.to_inclusive_bounds(), None);
    }
}
