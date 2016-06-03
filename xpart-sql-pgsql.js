"use strict";

const core = require("./xpart-sql");

const SQLError = core.SQLError;
const SQLDriver = core.SQLDriver;
const SQLClient = core.SQLClient;

const hasOwn = Object.prototype.hasOwnProperty;

// ============================================================================
// [Utils]
// ============================================================================

/**
 * Creates a new `PGSQLDriver` instance.
 */
function new_(app, config) {
  return new PGSQLDriver(app, config);
}
exports.new = new_;

/**
 * PostgreSQL TypeName to OID mapping.
 */
const OID = Object.freeze({
  BOOL                :   16,
  BYTEA               :   17,
  CHAR                :   18,
  NAME                :   19,
  INT8                :   20,
  INT2                :   21,
  INT2_VECTOR         :   22,
  INT4                :   23,
  REG_PROC            :   24,
  TEXT                :   25,
  OID                 :   26,
  TID                 :   27,
  XID                 :   28,
  CID                 :   29,
  OID_VECTOR          :   30,
  PG_DDL_COMMAND      :   32,
  PG_TYPE             :   71,
  PG_ATTRIBUTE        :   75,
  PG_PROC             :   81,
  PG_CLASS            :   83,
  JSON                :  114,
  XML                 :  142,
  XML_ARRAY           :  143,
  PG_NODE_TREE        :  194,
  JSON_ARRAY          :  199,
  SMGR                :  210,
  POINT               :  600,
  LSEG                :  601,
  PATH                :  602,
  BOX                 :  603,
  POLYGON             :  604,
  LINE                :  628,
  LINE_ARRAY          :  629,
  CIDR                :  650,
  CIDR_ARRAY          :  651,
  FLOAT4              :  700,
  FLOAT8              :  701,
  ABSTIME             :  702,
  RELTIME             :  703,
  TINTERVAL           :  704,
  UNKNOWN             :  705,
  CIRCLE              :  718,
  CIRCLE_ARRAY        :  719,
  MONEY               :  790,
  MONEY_ARRAY         :  791,
  MACADDR             :  829,
  INET                :  869,
  BOOL_ARRAY          : 1000,
  BYTEA_ARRAY         : 1001,
  CHAR_ARRAY          : 1002,
  NAME_ARRAY          : 1003,
  INT2_ARRAY          : 1005,
  INT2_VECTOR_ARRAY   : 1006,
  INT4_ARRAY          : 1007,
  REG_PROC_ARRAY      : 1008,
  TEXT_ARRAY          : 1009,
  TID_ARRAY           : 1010,
  XID_ARRAY           : 1011,
  CID_ARRAY           : 1012,
  OID_VECTOR_ARRAY    : 1013,
  BPCHAR_ARRAY        : 1014,
  VARCHAR_ARRAY       : 1015,
  INT8_ARRAY          : 1016,
  POINT_ARRAY         : 1017,
  LSEG_ARRAY          : 1018,
  PATH_ARRAY          : 1019,
  BOX_ARRAY           : 1020,
  FLOAT4_ARRAY        : 1021,
  FLOAT8_ARRAY        : 1022,
  ABSTIME_ARRAY       : 1023,
  RELTIME_ARRAY       : 1024,
  TINTERVAL_ARRAY     : 1025,
  POLYGON_ARRAY       : 1027,
  OID_ARRAY           : 1028,
  ACL_ITEM            : 1033, // AccessControlList
  ACL_ITEM_ARRAY      : 1034, // AccessControlList[]
  MACADDR_ARRAY       : 1040,
  INET_ARRAY          : 1041,
  BPCHAR              : 1042,
  VARCHAR             : 1043,
  DATE                : 1082,
  TIME                : 1083,
  TIMESTAMP           : 1114,
  TIMESTAMP_ARRAY     : 1115,
  DATE_ARRAY          : 1181,
  TIME_ARRAY          : 1183,
  TIMESTAMPTZ         : 1184,
  TIMESTAMPTZ_ARRAY   : 1185,
  INTERVAL            : 1186,
  INTERVAL_ARRAY      : 1187,
  NUMERIC_ARRAY       : 1231,
  CSTRING_ARRAY       : 1263,
  TIMETZ              : 1266,
  TIMETZ_ARRAY        : 1270,
  BIT                 : 1560,
  BIT_ARRAY           : 1561,
  VARBIT              : 1562,
  VARBIT_ARRAY        : 1563,
  NUMERIC             : 1700,
  REF_CURSOR          : 1790,
  REF_CURSOR_ARRAY    : 2201,
  REG_PROCEDURE       : 2202,
  REG_OPER            : 2203,
  REG_OPERATOR        : 2204,
  REG_CLASS           : 2205,
  REG_TYPE            : 2206,
  REG_PROCEDURE_ARRAY : 2207,
  REG_OPER_ARRAY      : 2208,
  REG_OEPRATOR_ARRAY  : 2209,
  REG_CLASS_ARRAY     : 2210,
  REG_TYPE_ARRAY      : 2211,
  RECORD              : 2249,
  CSTRING             : 2275,
  ANY                 : 2276,
  ANY_ARRAY           : 2277,
  VOID                : 2278,
  TRIGGER             : 2279,
  LANGUAGE_HANDLER    : 2280,
  INTERNAL            : 2281,
  OPAQUE              : 2282,
  ANY_ELEMENT         : 2283,
  RECORD_ARRAY        : 2287,
  ANY_NONARRAY        : 2776,
  TXID_SNAPSHOT_ARRAY : 2949,
  UUID                : 2950,
  UUID_ARRAY          : 2951,
  TXID_SNAPSHOT       : 2970,
  FDW_HANDLER         : 3115,
  PG_LSN              : 3220,
  PG_LSN_ARRAY        : 3221,
  TSM_HANDLER         : 3310,
  ANY_ENUM            : 3500,
  TS_VECTOR           : 3614,
  TS_QUERY            : 3615,
  GTS_VECTOR          : 3642,
  TS_VECTOR_ARRAY     : 3643,
  GTS_VECTOR_ARRAY    : 3644,
  REG_CONFIG          : 3734,
  REG_CONFIG_ARRAY    : 3735,
  REG_DICTIONARY      : 3769,
  REG_DICTIONARY_ARRAY: 3770,
  JSONB               : 3802,
  ANY_RANGE           : 3831,
  EVT_TRIGGER         : 3838,
  INT4_RANGE          : 3904,
  INT4_RANGE_ARRAY    : 3905,
  NUM_RANGE           : 3906,
  NUM_RANGE_ARRAY     : 3907,
  TS_RANGE            : 3908,
  TS_RANGE_ARRAY      : 3909,
  TSTZ_RANGE          : 3910,
  TSTZ_RANGE_ARRAY    : 3911,
  DATE_RANGE          : 3912,
  DATE_RANGE_ARRAY    : 3913,
  INT8_RANGE          : 3926,
  INT8_RANGE_ARRAY    : 3927,
  REG_NAMESPACE       : 4089,
  REG_NAMESPACE_ARRAY : 4090,
  REG_ROLE            : 4096,
  REG_ROLE_ARRAY      : 4097
});
exports.OID = OID;

/**
 * Normalizes an array of type parsers `input` into an array of objects where
 * each object has only `type` (OID), `format`, and `parsers`. It converts
 * types that were given as strings into the numeric equivalent used by the
 * `pg` module.
 *
 * @param {object[]} input Array of possibly unnormalized type parsers.
 * @return {object[]} Array of normalized type parsers.
 *
 * @throws {SQLError} If an invalid type (postgres-oid) is specified.
 */
function normalizeTypeParsers(input) {
  var output = [];

  for (var i = 0; i < input.length; i++) {
    var tp = input[i];

    var type = tp.type;
    var format = tp.format;
    var parser = tp.parser;

    if (typeof type === "string") {
      if (!hasOwn.call(OID, type))
        throw new SQLError("Cannot convert type '" + type + "' to OID: Not found");
      type = OID[type];
    }

    output.push({
      type  : type,
      format: format,
      parser: parser
    });
  }

  return output;
}
exports.normalizeTypeParsers = normalizeTypeParsers;

/**
 * @private
 */
function initTypeParsers(impl, input) {
  for (var i = 0; i < input.length; i++) {
    var tp = input[i];
    impl.setTypeParser(tp.type, tp.format, tp.parser);
  }
}

// ============================================================================
// [PGSQLDriver]
// ============================================================================

/**
 * PostgreSQL database driver.
 */
class PGSQLDriver extends SQLDriver {
  constructor(app, config) {
    super(app, config);

    var internal = this._internal;
    internal.dialect = "pgsql";

    // Generic options.
    this._postConfigure(config, {
      driver: "pg"
    });

    // PostgreSQL specific options.
    if (config.pgTypeParsers)
      internal.pgTypeParsers = normalizeTypeParsers(config.pgTypeParsers);
  }

  /** @override */
  _getServerURL() {
    var internal = this._internal;

    var uri = "postgres://" + internal.username + ":" + internal.password;
    uri += internal.host ? "@" + internal.host : "@localhost";
    uri += internal.port ? ":" + internal.port : "";
    uri += internal.database ? "/" + internal.database : "";
    return uri;
  }

  /** @override */
  _createClient(cb) {
    var self = this;

    var internal = this._internal;
    var impl = new internal.impl.Client(this._getServerURL());

    // PG type parsers.
    if (internal.pgTypeParsers)
      initTypeParsers(impl, internal.pgTypeParsers);

    impl.connect(function(err) {
      if (err)
        return cb(err, null);
      cb(null, new PGSQLClient(self, impl));
    });
  }

  /** @override */
  _destroyClient(client) {
    client._impl.end();
  }
}
exports.PGSQLDriver = PGSQLDriver;

// ============================================================================
// [PGSQLClient]
// ============================================================================

/**
 * PostgreSQL client.
 */
class PGSQLClient extends SQLClient {
  constructor(driver, impl) {
    super(driver, impl);

    this._onQuery = PGSQLClient.prototype._onQuery.bind(this);
  }

  /** @override */
  _query(qs, cb) {
    // This is a bit tricky. Because callbacks often have just `err, result`
    // signature the query string and the original callback are stored in the
    // client itself. This is fine as JS is single-threaded and clients are
    // not used concurrently. The `client._onQuery()` will clear these values
    // and call the real handler.
    this._qs = qs;
    this._cb = cb;

    this._impl.query(qs, this._onQuery);
  }

  /** @private */
  _onQuery(err, result) {
    // Get and purge the values stored by `_query()`.
    var qs = this._qs;
    var cb = this._cb;

    this._qs = "";
    this._cb = null;

    var driver = this._driver;
    var internal = driver._internal;

    var app = driver.app;
    var indent = "  ";

    // Release the client into the DB driver's pool if `_returnToPool` is set.
    if (this._returnToPool)
      driver._onClientIdle(this);

    // Handle all SQL errors together with the SQL query executed. This means
    // that we don't have to log messages in business logic as the error has
    // been already reported by the lower layer (SQL).
    if (err) {
      app.error("[xpart.sql] Query:\n" + qs + "\n",
                "[xpart.sql] " + err.toString());
      return cb(err, null);
    }

    if (internal.debugQueries)
      app.silly("[xpart.sql] Query:\n" + qs);

    if (internal.debugResults)
      app.silly(indent + JSON.stringify(result.rows, null, 2).replace(/\n/g, indent));

    return cb(null, {
      rows : result.rows || null,
      count: result.rowCount
    });
  }
}
exports.PGSQLClient = PGSQLClient;
