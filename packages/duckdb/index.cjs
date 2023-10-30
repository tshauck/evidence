const { getEnv } = require('@evidence-dev/db-commons');
const { Database, OPEN_READONLY, OPEN_READWRITE } = require('duckdb-async');

const envMap = {
	filename: [
		{ key: 'EVIDENCE_DUCKDB_FILENAME', deprecated: false },
		{ key: 'DUCKDB_FILENAME', deprecated: false },
		{ key: 'filename', deprecated: true },
		{ key: 'FILENAME', deprecated: true }
	]
};

function nativeTypeToEvidenceType(data) {
	switch (typeof data) {
		case 'number':
			return 'number';
		case 'string':
			return 'string';
		case 'bigint':
			return 'bigint';
		case 'boolean':
			return 'boolean';
		case 'object':
			if (data instanceof Date) {
				return 'date';
			}
			return undefined;
		default:
			return 'string';
	}
}

/**
 * Convert a BigInt value to a number.
 * If the value isn't a BigInt, returns the value unchanged.
 *
 * @param {*} value - The value to potentially convert.
 * @returns {*} - The converted number or the unchanged value.
 */
function convertBigIntToNumber(value) {
	if (typeof value === 'bigint') {
		return Number(value);
	}
	return value;
}

/**
 * Normalize a list of row objects, converting any BigInt values to numbers.
 *
 * @param {Object[]} rawRows - The rows to process.
 * @returns {Object[]} - The processed rows with BigInt values converted to numbers.
 */
function normalizeRows(rawRows) {
	for (const row of rawRows) {
		for (const key in row) {
			row[key] = convertBigIntToNumber(row[key]);
		}
	}
	return rawRows;
}

const mapResultsToEvidenceColumnTypes = function (rows) {
	const dataTypes = {};
	let typeFidelity = {};
	// Find the first row that has a value for each column.
	for (const row of rows) {
	  for (const [name, value] of Object.entries(row)) {
		if (!dataTypes[name]) {
		  const evidenceType = nativeTypeToEvidenceType(value);
		  if (evidenceType) {
			dataTypes[name] = evidenceType;
			typeFidelity[name] = 'precise';
		  }
		}
	  }
	  
	  // Check if all columns have been determined and stop iterating.
	  if (Object.keys(dataTypes).length === Object.keys(row).length) {
		break;
	  }
	}
  
	// If a column is still undefined, set it to 'string' as a default.
	for (const [name, value] of Object.entries(rows[0])) {
	  if (!dataTypes[name]) {
		dataTypes[name] = 'string';
		typeFidelity[name] = 'inferred';
	  }
	}
  
	return Object.entries(dataTypes).map(([name, evidenceType]) => {
	  return { name, evidenceType, typeFidelity: typeFidelity };
	});
};  

const runQuery = async (queryString, database) => {
	const filename = database ? database.filename : getEnv(envMap, 'filename');
	var filepath;
	if (filename.includes('.db') || filename.includes('.duckdb')) {
		filepath = '../../' + filename;
	} else {
		filepath = filename;
	}
	const mode = filename !== ':memory:' ? OPEN_READONLY : OPEN_READWRITE;

	try {
		const db = await Database.create(filepath, mode);
		const rawRows = await db.all(queryString); // renaming rows to rawRows for clarity
		const rows = normalizeRows(rawRows);

		return { rows, columnTypes: mapResultsToEvidenceColumnTypes(rows) };
	} catch (err) {
		if (err.message) {
			throw err.message;
		} else {
			throw err;
		}
	}
};

module.exports = runQuery;
