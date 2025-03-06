#include "duckdb/planner/operator/logical_pragma.hpp"

namespace duckdb {

idx_t LogicalPragma::EstimateCardinality(ClientContext &context) {
	return SetEstimatedCardinality(1);
}

} // namespace duckdb
