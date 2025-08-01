#include "duckdb/transaction/local_storage.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &table)
    : table_ref(table), allocator(Allocator::Get(table.db)), deleted_rows(0), optimistic_writer(context, table),
      merged_storage(false) {

	auto types = table.GetTypes();
	auto data_table_info = table.GetDataTableInfo();
	auto &io_manager = TableIOManager::Get(table);
	row_groups = make_shared_ptr<RowGroupCollection>(data_table_info, io_manager, types, MAX_ROW_ID, 0);
	row_groups->InitializeEmpty();

	data_table_info->GetIndexes().Scan([&](Index &index) {
		auto constraint = index.GetConstraintType();
		if (constraint == IndexConstraintType::NONE) {
			return false;
		}
		if (index.GetIndexType() != ART::TYPE_NAME) {
			return false;
		}
		if (!index.IsBound()) {
			return false;
		}
		auto &art = index.Cast<ART>();

		// UNIQUE constraint.
		vector<unique_ptr<Expression>> expressions;
		vector<unique_ptr<Expression>> delete_expressions;
		for (auto &expr : art.unbound_expressions) {
			expressions.push_back(expr->Copy());
			delete_expressions.push_back(expr->Copy());
		}

		// Create a delete index and a local index.
		auto &name = art.GetIndexName();
		auto &io_manager = art.table_io_manager;
		auto delete_index =
		    make_uniq<ART>(name, constraint, art.GetColumnIds(), io_manager, std::move(delete_expressions), art.db);
		delete_indexes.AddIndex(std::move(delete_index));

		auto append_index =
		    make_uniq<ART>(name, constraint, art.GetColumnIds(), io_manager, std::move(expressions), art.db);
		append_indexes.AddIndex(std::move(append_index));
		return false;
	});
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_data_table, LocalTableStorage &parent,
                                     const idx_t alter_column_index, const LogicalType &target_type,
                                     const vector<StorageIndex> &bound_columns, Expression &cast_expr)
    : table_ref(new_data_table), allocator(Allocator::Get(new_data_table.db)), deleted_rows(parent.deleted_rows),
      optimistic_collections(std::move(parent.optimistic_collections)),
      optimistic_writer(new_data_table, parent.optimistic_writer), merged_storage(parent.merged_storage) {

	// Alter the column type.
	row_groups = parent.row_groups->AlterType(context, alter_column_index, target_type, bound_columns, cast_expr);
	parent.row_groups->CommitDropColumn(alter_column_index);
	parent.row_groups.reset();

	append_indexes.Move(parent.append_indexes);
}

LocalTableStorage::LocalTableStorage(DataTable &new_data_table, LocalTableStorage &parent,
                                     const idx_t drop_column_index)
    : table_ref(new_data_table), allocator(Allocator::Get(new_data_table.db)), deleted_rows(parent.deleted_rows),
      optimistic_collections(std::move(parent.optimistic_collections)),
      optimistic_writer(new_data_table, parent.optimistic_writer), merged_storage(parent.merged_storage) {

	// Remove the column from the previous table storage.
	row_groups = parent.row_groups->RemoveColumn(drop_column_index);
	parent.row_groups->CommitDropColumn(drop_column_index);
	parent.row_groups.reset();

	append_indexes.Move(parent.append_indexes);
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_dt, LocalTableStorage &parent,
                                     ColumnDefinition &new_column, ExpressionExecutor &default_executor)
    : table_ref(new_dt), allocator(Allocator::Get(new_dt.db)), deleted_rows(parent.deleted_rows),
      optimistic_collections(std::move(parent.optimistic_collections)),
      optimistic_writer(new_dt, parent.optimistic_writer), merged_storage(parent.merged_storage) {

	row_groups = parent.row_groups->AddColumn(context, new_column, default_executor);
	parent.row_groups.reset();
	append_indexes.Move(parent.append_indexes);
}

LocalTableStorage::~LocalTableStorage() {
}

void LocalTableStorage::InitializeScan(CollectionScanState &state, optional_ptr<TableFilterSet> table_filters) {
	if (row_groups->GetTotalRows() == 0) {
		throw InternalException("No rows in LocalTableStorage row group for scan");
	}
	row_groups->InitializeScan(state, state.GetColumnIds(), table_filters.get());
}

idx_t LocalTableStorage::EstimatedSize() {
	// count the appended rows
	idx_t appended_rows = row_groups->GetTotalRows() - deleted_rows;

	// get the (estimated) size of a row (no compressions, etc.)
	idx_t row_size = 0;
	auto &types = row_groups->GetTypes();
	for (auto &type : types) {
		row_size += GetTypeIdSize(type.InternalType());
	}

	// get the index size
	idx_t index_sizes = 0;
	append_indexes.Scan([&](Index &index) {
		if (!index.IsBound()) {
			return false;
		}
		index_sizes += index.Cast<BoundIndex>().GetInMemorySize();
		return false;
	});

	// return the size of the appended rows and the index size
	return appended_rows * row_size + index_sizes;
}

void LocalTableStorage::WriteNewRowGroup() {
	if (deleted_rows != 0) {
		// we have deletes - we cannot merge row groups
		return;
	}
	optimistic_writer.WriteNewRowGroup(*row_groups);
}

void LocalTableStorage::FlushBlocks() {
	const idx_t row_group_size = row_groups->GetRowGroupSize();
	if (!merged_storage && row_groups->GetTotalRows() > row_group_size) {
		optimistic_writer.WriteLastRowGroup(*row_groups);
	}
	optimistic_writer.FinalFlush();
}

ErrorData LocalTableStorage::AppendToIndexes(DuckTransaction &transaction, RowGroupCollection &source,
                                             TableIndexList &index_list, const vector<LogicalType> &table_types,
                                             row_t &start_row) {
	// only need to scan for index append
	// figure out which columns we need to scan for the set of indexes
	auto index_columns = index_list.GetRequiredColumns();
	vector<StorageIndex> required_columns;
	for (auto &col : index_columns) {
		required_columns.emplace_back(col);
	}
	// create an empty mock chunk that contains all the correct types for the table
	DataChunk mock_chunk;
	mock_chunk.InitializeEmpty(table_types);
	ErrorData error;
	source.Scan(transaction, required_columns, [&](DataChunk &chunk) -> bool {
		// construct the mock chunk by referencing the required columns
		for (idx_t i = 0; i < required_columns.size(); i++) {
			auto col_id = required_columns[i].GetPrimaryIndex();
			mock_chunk.data[col_id].Reference(chunk.data[i]);
		}
		mock_chunk.SetCardinality(chunk);
		// append this chunk to the indexes of the table
		error = DataTable::AppendToIndexes(index_list, delete_indexes, mock_chunk, start_row, index_append_mode);
		if (error.HasError()) {
			return false;
		}
		start_row += UnsafeNumericCast<row_t>(chunk.size());
		return true;
	});
	return error;
}

void LocalTableStorage::AppendToIndexes(DuckTransaction &transaction, TableAppendState &append_state,
                                        bool append_to_table) {
	auto &table = table_ref.get();
	if (append_to_table) {
		table.InitializeAppend(transaction, append_state);
	}
	ErrorData error;
	if (append_to_table) {
		// appending: need to scan entire
		row_groups->Scan(transaction, [&](DataChunk &chunk) -> bool {
			// append this chunk to the indexes of the table
			error = table.AppendToIndexes(delete_indexes, chunk, append_state.current_row, index_append_mode);
			if (error.HasError()) {
				return false;
			}
			// append to base table
			table.Append(chunk, append_state);
			return true;
		});
	} else {
		auto data_table_info = table.GetDataTableInfo();
		auto &index_list = data_table_info->GetIndexes();
		error = AppendToIndexes(transaction, *row_groups, index_list, table.GetTypes(), append_state.current_row);
	}

	if (error.HasError()) {
		// need to revert all appended row ids
		row_t current_row = append_state.row_start;
		// remove the data from the indexes, if there are any indexes
		row_groups->Scan(transaction, [&](DataChunk &chunk) -> bool {
			// Remove this chunk from the indexes.
			try {
				table.RemoveFromIndexes(append_state, chunk, current_row);
			} catch (std::exception &ex) { // LCOV_EXCL_START
				error = ErrorData(ex);
				return false;
			} // LCOV_EXCL_STOP

			current_row += UnsafeNumericCast<row_t>(chunk.size());
			if (current_row >= append_state.current_row) {
				// finished deleting all rows from the index: abort now
				return false;
			}
			return true;
		});
		if (append_to_table) {
			table.RevertAppendInternal(NumericCast<idx_t>(append_state.row_start));
		}
#ifdef DEBUG
		// Verify that our index memory is stable.
		table.VerifyIndexBuffers();
#endif
		error.Throw();
	}
	if (append_to_table) {
		table.FinalizeAppend(transaction, append_state);
	}
}

PhysicalIndex LocalTableStorage::CreateOptimisticCollection(unique_ptr<RowGroupCollection> collection) {
	lock_guard<mutex> l(collections_lock);
	optimistic_collections.push_back(std::move(collection));
	return PhysicalIndex(optimistic_collections.size() - 1);
}

RowGroupCollection &LocalTableStorage::GetOptimisticCollection(const PhysicalIndex collection_index) {
	lock_guard<mutex> l(collections_lock);
	auto &collection = optimistic_collections[collection_index.index];
	return *collection;
}

void LocalTableStorage::ResetOptimisticCollection(const PhysicalIndex collection_index) {
	lock_guard<mutex> l(collections_lock);
	optimistic_collections[collection_index.index].reset();
}

OptimisticDataWriter &LocalTableStorage::GetOptimisticWriter() {
	return optimistic_writer;
}

void LocalTableStorage::Rollback() {
	optimistic_writer.Rollback();

	for (auto &collection : optimistic_collections) {
		if (!collection) {
			continue;
		}
		collection->CommitDropTable();
	}
	optimistic_collections.clear();
	row_groups->CommitDropTable();
}

//===--------------------------------------------------------------------===//
// LocalTableManager
//===--------------------------------------------------------------------===//
optional_ptr<LocalTableStorage> LocalTableManager::GetStorage(DataTable &table) const {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	return entry == table_storage.end() ? nullptr : entry->second.get();
}

LocalTableStorage &LocalTableManager::GetOrCreateStorage(ClientContext &context, DataTable &table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		auto new_storage = make_shared_ptr<LocalTableStorage>(context, table);
		auto storage = new_storage.get();
		table_storage.insert(make_pair(reference<DataTable>(table), std::move(new_storage)));
		return *storage;
	} else {
		return *entry->second.get();
	}
}

bool LocalTableManager::IsEmpty() const {
	lock_guard<mutex> l(table_storage_lock);
	return table_storage.empty();
}

shared_ptr<LocalTableStorage> LocalTableManager::MoveEntry(DataTable &table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		return nullptr;
	}
	auto storage_entry = std::move(entry->second);
	table_storage.erase(entry);
	return storage_entry;
}

reference_map_t<DataTable, shared_ptr<LocalTableStorage>> LocalTableManager::MoveEntries() {
	lock_guard<mutex> l(table_storage_lock);
	return std::move(table_storage);
}

idx_t LocalTableManager::EstimatedSize() const {
	lock_guard<mutex> l(table_storage_lock);
	idx_t estimated_size = 0;
	for (auto &storage : table_storage) {
		estimated_size += storage.second->EstimatedSize();
	}
	return estimated_size;
}

void LocalTableManager::InsertEntry(DataTable &table, shared_ptr<LocalTableStorage> entry) {
	lock_guard<mutex> l(table_storage_lock);
	D_ASSERT(table_storage.find(table) == table_storage.end());
	table_storage[table] = std::move(entry);
}

//===--------------------------------------------------------------------===//
// LocalStorage
//===--------------------------------------------------------------------===//
LocalStorage::LocalStorage(ClientContext &context, DuckTransaction &transaction)
    : context(context), transaction(transaction) {
}

LocalStorage::CommitState::CommitState() {
}

LocalStorage::CommitState::~CommitState() {
}

LocalStorage &LocalStorage::Get(DuckTransaction &transaction) {
	return transaction.GetLocalStorage();
}

LocalStorage &LocalStorage::Get(ClientContext &context, AttachedDatabase &db) {
	return DuckTransaction::Get(context, db).GetLocalStorage();
}

LocalStorage &LocalStorage::Get(ClientContext &context, Catalog &catalog) {
	return LocalStorage::Get(context, catalog.GetAttached());
}

void LocalStorage::InitializeScan(DataTable &table, CollectionScanState &state,
                                  optional_ptr<TableFilterSet> table_filters) {
	auto storage = table_manager.GetStorage(table);
	if (storage == nullptr || storage->row_groups->GetTotalRows() == 0) {
		return;
	}
	storage->InitializeScan(state, table_filters);
}

void LocalStorage::Scan(CollectionScanState &state, const vector<StorageIndex> &, DataChunk &result) {
	state.Scan(transaction, result);
}

void LocalStorage::InitializeParallelScan(DataTable &table, ParallelCollectionScanState &state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		state.max_row = 0;
		state.vector_index = 0;
		state.current_row_group = nullptr;
	} else {
		storage->row_groups->InitializeParallelScan(state);
	}
}

bool LocalStorage::NextParallelScan(ClientContext &context, DataTable &table, ParallelCollectionScanState &state,
                                    CollectionScanState &scan_state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return false;
	}
	return storage->row_groups->NextParallelScan(context, state, scan_state);
}

void LocalStorage::InitializeAppend(LocalAppendState &state, DataTable &table) {
	state.storage = &table_manager.GetOrCreateStorage(context, table);
	state.storage->row_groups->InitializeAppend(TransactionData(transaction), state.append_state);
}

void LocalStorage::InitializeStorage(LocalAppendState &state, DataTable &table) {
	state.storage = &table_manager.GetOrCreateStorage(context, table);
}

void LocalTableStorage::AppendToDeleteIndexes(Vector &row_ids, DataChunk &delete_chunk) {
	if (delete_chunk.size() == 0) {
		return;
	}

	delete_indexes.Scan([&](Index &index) {
		D_ASSERT(index.IsBound());
		if (!index.IsUnique()) {
			return false;
		}
		IndexAppendInfo index_append_info(IndexAppendMode::IGNORE_DUPLICATES, nullptr);
		auto result = index.Cast<BoundIndex>().Append(delete_chunk, row_ids, index_append_info);
		if (result.HasError()) {
			throw InternalException("unexpected constraint violation on delete ART: ", result.Message());
		}
		return false;
	});
}

void LocalStorage::Append(LocalAppendState &state, DataChunk &chunk) {
	// Append to any unique indexes.
	auto storage = state.storage;
	auto offset = NumericCast<idx_t>(MAX_ROW_ID) + storage->row_groups->GetTotalRows();
	idx_t base_id = offset + state.append_state.total_append_count;

	auto error = DataTable::AppendToIndexes(storage->append_indexes, storage->delete_indexes, chunk,
	                                        NumericCast<row_t>(base_id), storage->index_append_mode);
	if (error.HasError()) {
		error.Throw();
	}

	// Append the chunk to the local storage.
	auto new_row_group = storage->row_groups->Append(chunk, state.append_state);

	// Check if we should pre-emptively flush blocks to disk.
	if (new_row_group) {
		storage->WriteNewRowGroup();
	}
}

void LocalStorage::FinalizeAppend(LocalAppendState &state) {
	state.storage->row_groups->FinalizeAppend(state.append_state.transaction, state.append_state);
}

void LocalStorage::LocalMerge(DataTable &table, RowGroupCollection &collection) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	if (!storage.append_indexes.Empty()) {
		// append data to indexes if required
		row_t base_id = MAX_ROW_ID + NumericCast<row_t>(storage.row_groups->GetTotalRows());
		auto error =
		    storage.AppendToIndexes(transaction, collection, storage.append_indexes, table.GetTypes(), base_id);
		if (error.HasError()) {
			error.Throw();
		}
	}
	storage.row_groups->MergeStorage(collection, nullptr, nullptr);
	storage.merged_storage = true;
}

PhysicalIndex LocalStorage::CreateOptimisticCollection(DataTable &table, unique_ptr<RowGroupCollection> collection) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	return storage.CreateOptimisticCollection(std::move(collection));
}

RowGroupCollection &LocalStorage::GetOptimisticCollection(DataTable &table, const PhysicalIndex collection_index) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	return storage.GetOptimisticCollection(collection_index);
}

void LocalStorage::ResetOptimisticCollection(DataTable &table, const PhysicalIndex collection_index) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	storage.ResetOptimisticCollection(collection_index);
}

OptimisticDataWriter &LocalStorage::GetOptimisticWriter(DataTable &table) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	return storage.GetOptimisticWriter();
}

bool LocalStorage::ChangesMade() noexcept {
	return !table_manager.IsEmpty();
}

bool LocalStorage::Find(DataTable &table) {
	return table_manager.GetStorage(table) != nullptr;
}

idx_t LocalStorage::EstimatedSize() {
	return table_manager.EstimatedSize();
}

idx_t LocalStorage::Delete(DataTable &table, Vector &row_ids, idx_t count) {
	auto storage = table_manager.GetStorage(table);
	D_ASSERT(storage);

	// delete from unique indices (if any)
	if (!storage->append_indexes.Empty()) {
		storage->row_groups->RemoveFromIndexes(storage->append_indexes, row_ids, count);
	}

	auto ids = FlatVector::GetData<row_t>(row_ids);
	idx_t delete_count = storage->row_groups->Delete(TransactionData(0, 0), table, ids, count);
	storage->deleted_rows += delete_count;
	return delete_count;
}

void LocalStorage::Update(DataTable &table, Vector &row_ids, const vector<PhysicalIndex> &column_ids,
                          DataChunk &updates) {
	D_ASSERT(updates.size() >= 1);
	auto storage = table_manager.GetStorage(table);
	D_ASSERT(storage);

	auto ids = FlatVector::GetData<row_t>(row_ids);
	storage->row_groups->Update(TransactionData(0, 0), ids, column_ids, updates);
}

void LocalStorage::Flush(DataTable &table, LocalTableStorage &storage, optional_ptr<StorageCommitState> commit_state) {
	if (storage.is_dropped) {
		return;
	}
	if (storage.row_groups->GetTotalRows() <= storage.deleted_rows) {
		// all rows that we added were deleted
		// rollback any partial blocks that are still outstanding
		storage.Rollback();
		return;
	}

	auto append_count = storage.row_groups->GetTotalRows() - storage.deleted_rows;
	const auto row_group_size = storage.row_groups->GetRowGroupSize();

	TableAppendState append_state;
	table.AppendLock(append_state);
	transaction.PushAppend(table, NumericCast<idx_t>(append_state.row_start), append_count);
	if ((append_state.row_start == 0 || storage.row_groups->GetTotalRows() >= row_group_size) &&
	    storage.deleted_rows == 0) {
		// table is currently empty OR we are bulk appending: move over the storage directly
		// first flush any outstanding blocks
		storage.FlushBlocks();
		// Append to the indexes.
		if (table.HasIndexes()) {
			storage.AppendToIndexes(transaction, append_state, false);
		}
		// finally move over the row groups
		table.MergeStorage(*storage.row_groups, storage.append_indexes, commit_state);
	} else {
		// check if we have written data
		// if we have, we cannot merge to disk after all
		// so we need to revert the data we have already written
		storage.Rollback();
		// append to the indexes and append to the base table
		storage.AppendToIndexes(transaction, append_state, true);
	}

#ifdef DEBUG
	// Verify that our index memory is stable.
	table.VerifyIndexBuffers();
#endif
}

void LocalStorage::Commit(optional_ptr<StorageCommitState> commit_state) {
	// commit local storage
	// iterate over all entries in the table storage map and commit them
	// after this, the local storage is no longer required and can be cleared
	auto table_storage = table_manager.MoveEntries();
	for (auto &entry : table_storage) {
		auto table = entry.first;
		auto storage = entry.second.get();
		Flush(table, *storage, commit_state);
		entry.second.reset();
	}
}

void LocalStorage::Rollback() {
	// rollback local storage
	// after this, the local storage is no longer required and can be cleared
	auto table_storage = table_manager.MoveEntries();
	for (auto &entry : table_storage) {
		auto storage = entry.second.get();
		if (!storage) {
			continue;
		}
		storage->Rollback();
		entry.second.reset();
	}
}

idx_t LocalStorage::AddedRows(DataTable &table) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return 0;
	}
	return storage->row_groups->GetTotalRows() - storage->deleted_rows;
}

vector<PartitionStatistics> LocalStorage::GetPartitionStats(DataTable &table) const {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return vector<PartitionStatistics>();
	}
	return storage->row_groups->GetPartitionStats();
}

void LocalStorage::DropTable(DataTable &table) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return;
	}
	storage->is_dropped = true;
}

void LocalStorage::MoveStorage(DataTable &old_dt, DataTable &new_dt) {
	// check if there are any pending appends for the old version of the table
	auto new_storage = table_manager.MoveEntry(old_dt);
	if (!new_storage) {
		return;
	}
	// take over the storage from the old entry
	new_storage->table_ref = new_dt;
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::AddColumn(DataTable &old_dt, DataTable &new_dt, ColumnDefinition &new_column,
                             ExpressionExecutor &default_executor) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage = make_shared_ptr<LocalTableStorage>(context, new_dt, *storage, new_column, default_executor);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::DropColumn(DataTable &old_dt, DataTable &new_dt, const idx_t drop_column_index) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage = make_shared_ptr<LocalTableStorage>(new_dt, *storage, drop_column_index);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::ChangeType(DataTable &old_dt, DataTable &new_dt, idx_t changed_idx, const LogicalType &target_type,
                              const vector<StorageIndex> &bound_columns, Expression &cast_expr) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage = make_shared_ptr<LocalTableStorage>(context, new_dt, *storage, changed_idx, target_type,
	                                                      bound_columns, cast_expr);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::FetchChunk(DataTable &table, Vector &row_ids, idx_t count, const vector<StorageIndex> &col_ids,
                              DataChunk &chunk, ColumnFetchState &fetch_state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		throw InternalException("LocalStorage::FetchChunk - local storage not found");
	}
	storage->row_groups->Fetch(transaction, chunk, col_ids, row_ids, count, fetch_state);
}

bool LocalStorage::CanFetch(DataTable &table, const row_t row_id) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		throw InternalException("LocalStorage::CanFetch - local storage not found");
	}
	return storage->row_groups->CanFetch(transaction, row_id);
}

TableIndexList &LocalStorage::GetIndexes(ClientContext &context, DataTable &table) {
	auto &storage = table_manager.GetOrCreateStorage(context, table);
	return storage.append_indexes;
}

optional_ptr<LocalTableStorage> LocalStorage::GetStorage(DataTable &table) {
	return table_manager.GetStorage(table);
}

void LocalStorage::VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint) {
	auto storage = table_manager.GetStorage(parent);
	if (!storage) {
		return;
	}
	storage->row_groups->VerifyNewConstraint(parent, constraint);
}

} // namespace duckdb
