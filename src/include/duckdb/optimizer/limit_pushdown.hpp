//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/limit_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

class LogicalOperator;
class Optimizer;

class LimitPushdown {
public:
//	explicit LimitPushdown(Optimizer &optimizer);

	//! Perform limit pushdown
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether we can perform the optimization on this operator
	static bool CanOptimize(LogicalOperator &op);
};

} // namespace duckdb
