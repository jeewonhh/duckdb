//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/cast_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/operator/convert_to_string.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {
struct CastParameters;
struct ValidityMask;
class Vector;

struct TryCast {
	static ConversionException UnimplementedErrorMessage(PhysicalType source, PhysicalType target,
	                                                     optional_ptr<CastParameters> parameters);
	static string UnimplementedCastMessage(const LogicalType &source, const LogicalType &target);

	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		throw UnimplementedErrorMessage(GetTypeId<SRC>(), GetTypeId<DST>(), nullptr);
	}
};

struct TryCastErrorMessage {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, CastParameters &parameters) {
		throw TryCast::UnimplementedErrorMessage(GetTypeId<SRC>(), GetTypeId<DST>(), parameters);
	}
};

struct TryCastErrorMessageCommaSeparated {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, CastParameters &parameters) {
		throw TryCast::UnimplementedErrorMessage(GetTypeId<SRC>(), GetTypeId<DST>(), parameters);
	}
};

template <class SRC, class DST>
static string CastExceptionText(SRC input) {
	if (std::is_same<SRC, string_t>()) {
		return "Could not convert string '" + ConvertToString::Operation<SRC>(input) + "' to " +
		       TypeIdToString(GetTypeId<DST>());
	}
	if (TypeIsNumber<SRC>() && TypeIsNumber<DST>()) {
		return "Type " + TypeIdToString(GetTypeId<SRC>()) + " with value " + ConvertToString::Operation<SRC>(input) +
		       " can't be cast because the value is out of range for the destination type " +
		       TypeIdToString(GetTypeId<DST>());
	}
	return "Type " + TypeIdToString(GetTypeId<SRC>()) + " with value " + ConvertToString::Operation<SRC>(input) +
	       " can't be cast to the destination type " + TypeIdToString(GetTypeId<DST>());
}

struct Cast {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		DST result;
		if (!TryCast::Operation(input, result)) {
			throw InvalidInputException(CastExceptionText<SRC, DST>(input));
		}
		return result;
	}
};

struct HandleCastError {
	static void AssignError(const string &error_message, CastParameters &parameters);
	static void AssignError(const string &error_message, string *error_message_ptr,
	                        optional_ptr<const Expression> cast_source = nullptr,
	                        optional_idx error_location = optional_idx());
};

//===--------------------------------------------------------------------===//
// Cast bool -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(bool input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast hugeint_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uhugeint_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast float -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(float input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast double -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(double input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// String -> Numeric Casts
//===--------------------------------------------------------------------===//
static inline bool TryCastStringBool(const char *input_data, idx_t input_size, bool &result, bool strict) {
	switch (input_size) {
	case 1: {
		unsigned char c = static_cast<uint8_t>(std::tolower(*input_data));
		if (c == 't' || (!strict && c == 'y') || (!strict && c == '1')) {
			result = true;
			return true;
		} else if (c == 'f' || (!strict && c == 'n') || (!strict && c == '0')) {
			result = false;
			return true;
		}
		return false;
	}
	case 2: {
		unsigned char n = static_cast<uint8_t>(std::tolower(input_data[0]));
		unsigned char o = static_cast<uint8_t>(std::tolower(input_data[1]));
		if (n == 'n' && o == 'o') {
			result = false;
			return true;
		}
		return false;
	}
	case 3: {
		unsigned char y = static_cast<uint8_t>(std::tolower(input_data[0]));
		unsigned char e = static_cast<uint8_t>(std::tolower(input_data[1]));
		unsigned char s = static_cast<uint8_t>(std::tolower(input_data[2]));
		if (y == 'y' && e == 'e' && s == 's') {
			result = true;
			return true;
		}
		return false;
	}
	case 4: {
		unsigned char t = static_cast<uint8_t>(std::tolower(input_data[0]));
		unsigned char r = static_cast<uint8_t>(std::tolower(input_data[1]));
		unsigned char u = static_cast<uint8_t>(std::tolower(input_data[2]));
		unsigned char e = static_cast<uint8_t>(std::tolower(input_data[3]));
		if (t == 't' && r == 'r' && u == 'u' && e == 'e') {
			result = true;
			return true;
		}
		return false;
	}
	case 5: {
		unsigned char f = static_cast<uint8_t>(std::tolower(input_data[0]));
		unsigned char a = static_cast<uint8_t>(std::tolower(input_data[1]));
		unsigned char l = static_cast<uint8_t>(std::tolower(input_data[2]));
		unsigned char s = static_cast<uint8_t>(std::tolower(input_data[3]));
		unsigned char e = static_cast<uint8_t>(std::tolower(input_data[4]));
		if (f == 'f' && a == 'a' && l == 'l' && s == 's' && e == 'e') {
			result = false;
			return true;
		}
		return false;
	}
	default:
		return false;
	}
}

template <>
DUCKDB_API bool TryCast::Operation(string_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, double &result, bool strict);
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, float &result, CastParameters &parameters);
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, double &result, CastParameters &parameters);
template <>
DUCKDB_API bool TryCastErrorMessageCommaSeparated::Operation(string_t input, float &result, CastParameters &parameters);
template <>
DUCKDB_API bool TryCastErrorMessageCommaSeparated::Operation(string_t input, double &result,
                                                             CastParameters &parameters);

//===--------------------------------------------------------------------===//
// Date Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(date_t input, date_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(date_t input, timestamp_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Time Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(dtime_t input, dtime_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(dtime_t input, dtime_tz_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Time (ns) Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(dtime_t input, dtime_ns_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(dtime_ns_t input, dtime_ns_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(dtime_ns_t input, dtime_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Time With Time Zone Casts (Offset)
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(dtime_tz_t input, dtime_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(dtime_tz_t input, dtime_tz_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Timestamp Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, date_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, dtime_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, dtime_tz_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, timestamp_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_sec_t input, timestamp_sec_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, timestamp_sec_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_ms_t input, timestamp_ms_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, timestamp_ms_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_ns_t input, timestamp_ns_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, timestamp_ns_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_tz_t input, timestamp_tz_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, timestamp_tz_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Interval Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(interval_t input, interval_t &result, bool strict);

//===--------------------------------------------------------------------===//
// String -> Date Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, date_t &result, CastParameters &parameters);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, date_t &result, bool strict);
template <>
date_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> Time Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, dtime_t &result, CastParameters &parameters);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, dtime_t &result, bool strict);
template <>
dtime_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> Time_NS Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, dtime_ns_t &result, CastParameters &parameters);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, dtime_ns_t &result, bool strict);
template <>
dtime_ns_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> TimeTZ Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, dtime_tz_t &result, CastParameters &parameters);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, dtime_tz_t &result, bool strict);
template <>
dtime_tz_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> Timestamp Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, timestamp_t &result, CastParameters &parameters);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, timestamp_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, timestamp_ns_t &result, bool strict);
template <>
timestamp_t Cast::Operation(string_t input);
template <>
timestamp_ns_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> Interval Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, interval_t &result, CastParameters &parameters);

//===--------------------------------------------------------------------===//
// string -> Non-Standard Timestamps
//===--------------------------------------------------------------------===//
struct TryCastToTimestampNS {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		throw InternalException("Unsupported type for try cast to timestamp (ns)");
	}
};

struct TryCastToTimestampMS {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		throw InternalException("Unsupported type for try cast to timestamp (ms)");
	}
};

struct TryCastToTimestampSec {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		throw InternalException("Unsupported type for try cast to timestamp (s)");
	}
};

template <>
DUCKDB_API bool TryCastToTimestampNS::Operation(string_t input, timestamp_ns_t &result, bool strict);
template <>
DUCKDB_API bool TryCastToTimestampMS::Operation(string_t input, timestamp_t &result, bool strict);
template <>
DUCKDB_API bool TryCastToTimestampSec::Operation(string_t input, timestamp_t &result, bool strict);

template <>
DUCKDB_API bool TryCastToTimestampNS::Operation(date_t input, timestamp_ns_t &result, bool strict);
template <>
DUCKDB_API bool TryCastToTimestampMS::Operation(date_t input, timestamp_t &result, bool strict);
template <>
DUCKDB_API bool TryCastToTimestampSec::Operation(date_t input, timestamp_t &result, bool strict);

//===--------------------------------------------------------------------===//
// string -> Non-Standard Time types
//===--------------------------------------------------------------------===//
struct TryCastToTimeNS {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		throw InternalException("Unsupported type for try cast to time (ns)");
	}
};

template <>
DUCKDB_API bool TryCastToTimeNS::Operation(string_t input, dtime_ns_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Non-Standard Time -> string/time types
//===--------------------------------------------------------------------===//

struct CastFromTimeNS {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to string could not be performed!");
	}
};

template <>
duckdb::string_t CastFromTimeNS::Operation(duckdb::dtime_ns_t input, Vector &result);

//===--------------------------------------------------------------------===//
// Non-Standard Timestamps -> string/timestamp types
//===--------------------------------------------------------------------===//

struct CastFromTimestampNS {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to string could not be performed!");
	}
};

struct CastFromTimestampMS {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to string could not be performed!");
	}
};

struct CastFromTimestampSec {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to string could not be performed!");
	}
};

struct CastTimestampUsToMs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampUsToNs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampUsToSec {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampMsToDate {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to DATE could not be performed!");
	}
};

struct CastTimestampMsToTime {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIME could not be performed!");
	}
};

struct CastTimestampMsToUs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampMsToNs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIMESTAMP_NS could not be performed!");
	}
};

struct CastTimestampNsToDate {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to DATE could not be performed!");
	}
};
struct CastTimestampNsToTime {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIME could not be performed!");
	}
};
struct CastTimestampNsToTimeNs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIME_NS could not be performed!");
	}
};
struct CastTimestampNsToUs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampSecToDate {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to DATE could not be performed!");
	}
};
struct CastTimestampSecToTime {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIME could not be performed!");
	}
};
struct CastTimestampSecToMs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIMESTAMP_MS could not be performed!");
	}
};

struct CastTimestampSecToUs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampSecToNs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIMESTAMP_NS could not be performed!");
	}
};

template <>
duckdb::timestamp_t CastTimestampUsToSec::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampUsToMs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampUsToNs::Operation(duckdb::timestamp_t input);
template <>
duckdb::date_t CastTimestampMsToDate::Operation(duckdb::timestamp_t input);
template <>
duckdb::dtime_t CastTimestampMsToTime::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampMsToUs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampMsToNs::Operation(duckdb::timestamp_t input);
template <>
duckdb::date_t CastTimestampNsToDate::Operation(duckdb::timestamp_t input);
template <>
duckdb::dtime_t CastTimestampNsToTime::Operation(duckdb::timestamp_t input);
template <>
duckdb::dtime_ns_t CastTimestampNsToTimeNs::Operation(duckdb::timestamp_ns_t input);
template <>
duckdb::timestamp_t CastTimestampNsToUs::Operation(duckdb::timestamp_t input);
template <>
duckdb::date_t CastTimestampSecToDate::Operation(duckdb::timestamp_t input);
template <>
duckdb::dtime_t CastTimestampSecToTime::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampSecToMs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampSecToUs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampSecToNs::Operation(duckdb::timestamp_t input);

template <>
duckdb::string_t CastFromTimestampNS::Operation(duckdb::timestamp_ns_t input, Vector &result);
template <>
duckdb::string_t CastFromTimestampMS::Operation(duckdb::timestamp_t input, Vector &result);
template <>
duckdb::string_t CastFromTimestampSec::Operation(duckdb::timestamp_t input, Vector &result);

//===--------------------------------------------------------------------===//
// Blobs
//===--------------------------------------------------------------------===//
struct CastFromBlob {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from blob could not be performed!");
	}
};
template <>
duckdb::string_t CastFromBlob::Operation(duckdb::string_t input, Vector &vector);

struct CastFromBlobToBit {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw NotImplementedException("Cast from blob could not be performed!");
	}
};
template <>
string_t CastFromBlobToBit::Operation(string_t input, Vector &result);

struct TryCastToBlob {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, CastParameters &parameters) {
		throw InternalException("Unsupported type for try cast to blob");
	}
};
template <>
bool TryCastToBlob::Operation(string_t input, string_t &result, Vector &result_vector, CastParameters &parameters);

//===--------------------------------------------------------------------===//
// Bits
//===--------------------------------------------------------------------===//
struct CastFromBitToString {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from bit could not be performed!");
	}
};
template <>
duckdb::string_t CastFromBitToString::Operation(duckdb::string_t input, Vector &vector);

struct CastFromBitToNumeric {
	template <class SRC = string_t, class DST>
	static inline bool Operation(SRC input, DST &result, CastParameters &parameters) {
		D_ASSERT(input.GetSize() > 1);

		// TODO: Allow conversion if the significant bytes of the bitstring can be cast to the target type
		// Currently only allows bitstring -> numeric if the full bitstring fits inside the numeric type
		if (input.GetSize() - 1 > sizeof(DST)) {
			throw ConversionException(parameters.query_location, "Bitstring doesn't fit inside of %s",
			                          GetTypeId<DST>());
		}
		Bit::BitToNumeric(input, result);
		return (true);
	}
};
template <>
bool CastFromBitToNumeric::Operation(string_t input, bool &result, CastParameters &parameters);
template <>
bool CastFromBitToNumeric::Operation(string_t input, hugeint_t &result, CastParameters &parameters);
template <>
bool CastFromBitToNumeric::Operation(string_t input, uhugeint_t &result, CastParameters &parameters);

struct CastFromBitToBlob {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		D_ASSERT(input.GetSize() > 1);
		return StringVector::AddStringOrBlob(result, Bit::BitToBlob(input));
	}
};

struct TryCastToBit {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, CastParameters &parameters) {
		throw InternalException("Unsupported type for try cast to bit");
	}
};

template <>
bool TryCastToBit::Operation(string_t input, string_t &result, Vector &result_vector, CastParameters &parameters);

//===--------------------------------------------------------------------===//
// UUID
//===--------------------------------------------------------------------===//
struct CastFromUUID {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from uuid could not be performed!");
	}
};
template <>
duckdb::string_t CastFromUUID::Operation(duckdb::hugeint_t input, Vector &vector);

struct TryCastToUUID {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, CastParameters &parameters) {
		throw InternalException("Unsupported type for try cast to uuid");
	}
};

template <>
DUCKDB_API bool TryCastToUUID::Operation(string_t input, hugeint_t &result, Vector &result_vector,
                                         CastParameters &parameters);

//===--------------------------------------------------------------------===//
// Pointers
//===--------------------------------------------------------------------===//
struct CastFromPointer {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from pointer could not be performed!");
	}
};
template <>
duckdb::string_t CastFromPointer::Operation(uintptr_t input, Vector &vector);

} // namespace duckdb
