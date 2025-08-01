//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_opener.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/setting_info.hpp"

namespace duckdb {

struct CatalogTransaction;
class SecretManager;
class ClientContext;
class HTTPUtil;
class Value;
class Logger;

struct FileOpenerInfo {
	string file_path;
};

//! Abstract type that provide client-specific context to FileSystem.
class FileOpener {
public:
	FileOpener() {
	}
	virtual ~FileOpener() {};

	virtual SettingLookupResult TryGetCurrentSetting(const string &key, Value &result, FileOpenerInfo &info);
	virtual SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) = 0;
	virtual optional_ptr<ClientContext> TryGetClientContext() = 0;
	virtual optional_ptr<DatabaseInstance> TryGetDatabase() = 0;
	virtual shared_ptr<HTTPUtil> &GetHTTPUtil() = 0;

	DUCKDB_API virtual Logger &GetLogger() const = 0;
	DUCKDB_API static unique_ptr<CatalogTransaction> TryGetCatalogTransaction(optional_ptr<FileOpener> opener);
	DUCKDB_API static optional_ptr<ClientContext> TryGetClientContext(optional_ptr<FileOpener> opener);
	DUCKDB_API static optional_ptr<DatabaseInstance> TryGetDatabase(optional_ptr<FileOpener> opener);
	DUCKDB_API static optional_ptr<SecretManager> TryGetSecretManager(optional_ptr<FileOpener> opener);
	DUCKDB_API static SettingLookupResult TryGetCurrentSetting(optional_ptr<FileOpener> opener, const string &key,
	                                                           Value &result);
	DUCKDB_API static SettingLookupResult TryGetCurrentSetting(optional_ptr<FileOpener> opener, const string &key,
	                                                           Value &result, FileOpenerInfo &info);

	template <class TYPE>
	static SettingLookupResult TryGetCurrentSetting(optional_ptr<FileOpener> opener, const string &key, TYPE &result,
	                                                optional_ptr<FileOpenerInfo> info) {
		Value output;
		SettingLookupResult lookup_result;

		if (info) {
			lookup_result = TryGetCurrentSetting(opener, key, output, *info);
		} else {
			lookup_result = TryGetCurrentSetting(opener, key, output, *info);
		}

		if (lookup_result) {
			result = output.GetValue<TYPE>();
		}
		return lookup_result;
	}
};

} // namespace duckdb
