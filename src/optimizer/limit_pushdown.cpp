#include "duckdb/optimizer/limit_pushdown.hpp"

#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include <iostream>

namespace duckdb {

bool LimitPushdown::CanOptimize(duckdb::LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT &&
	    op.children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &limit = op.Cast<LogicalLimit>();

		if (limit.offset_val.Type() == LimitNodeType::EXPRESSION_PERCENTAGE ||
		    limit.offset_val.Type() == LimitNodeType::EXPRESSION_VALUE) {
			// Offset cannot be an expression
			return false;
		}

		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE && limit.limit_val.GetConstantValue() < 8192) {
			// Push down only when limit value is small (< 8192).
			return true;
		}
	}
	return false;
}

unique_ptr<LogicalOperator> LimitPushdown::Optimize(unique_ptr<LogicalOperator> op) {
	if (CanOptimize(*op)) {
		auto projection = std::move(op->children[0]);
		op->children[0] = std::move(projection->children[0]);
		projection->children[0] = std::move(op);
		swap(projection, op);
		//		auto &limit = op->Cast<LogicalLimit>();
		//		auto &proj = (op->children[0])->Cast<LogicalProjection>();
		//		auto newProj = make_uniq<LogicalProjection>(proj.table_index, std::move(proj.expressions));
		//		auto newLimit = make_uniq<LogicalLimit>(std::move(limit.limit_val), std::move(limit.offset_val));
		//		newLimit->AddChild(std::move(proj.children[0]));
		//		newProj->AddChild(std::move(newLimit));
		//		op = std::move(newProj);
	} else {
		for (auto &child : op->children) {
			child = Optimize(std::move(child));
		}
	}
	return op;
}

} // namespace duckdb
