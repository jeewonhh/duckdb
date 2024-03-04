#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPushdown::PushdownLimit(unique_ptr<LogicalOperator> op) {
	auto &limit = op->Cast<LogicalLimit>();

	if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE && limit.limit_val.GetConstantValue() == 0) {
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}

	if (op->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		if ((limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE &&
		    limit.offset_val.Type() != LimitNodeType::EXPRESSION_VALUE) ||
		    ((limit.limit_val.Type() == LimitNodeType::UNSET &&
		      limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE))) {
			auto projection = std::move(op->children[0]);
			op->children[0] = std::move(projection->children[0]);
			projection->children[0] = std::move(op);
			swap(projection, op);
		}
	}

	return FinishPushdown(std::move(op));
}

} // namespace duckdb
