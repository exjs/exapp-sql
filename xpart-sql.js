"use strict";

function new_(app, config) {
  const engine = getEngine(config.engine);
  return engine.new(app, config);
}
exports.new = new_;

/**
 * Gets the SQL engine by `name`.
 *
 * @param {string} name Name of the SQL engine to get.
 * @return {object} SQL module.
 *
 * @throws {Error} If the requested SQL module doesn't exist.
 */
function getEngine(name) {
  if (!/[a-z_][a-z0-9_]*/.test(name))
    throw new Error("Invalid module '" + name + "'");

  return require("./xpart-sql-" + name + ".js");
}
exports.getEngine = getEngine;

const nopCompiler = new class {
  compile(q) { return String(q); }
};

// ============================================================================
// [SQLError]
// ============================================================================

/**
 * SQL error that can be returned by an underlying SQL driver.
 *
 * @param message Error mesasge.
 */
class SQLError extends Error {
  constructor(message) {
    super(message);
    this.name = "SQLError";
    this.message = message;
  }
}
exports.SQLError = SQLError;

// ============================================================================
// [SQLDriver]
// ============================================================================

/**
 * Base class that implements functionality required by all SQL drivers.
 */
class SQLDriver {
  constructor(app, config) {
    const self = this;

    // Internals are always used by driver implementations.
    this.app = app;
    this._internal = {
      impl           : config.backend || null,       // SQL driver module (pg, mysql, etc).
      dialect        : "",                           // SQL driver dialect ("pgsql", "mysql", etc).
      status         : "pending",                    // SQL driver status.

      compiler       : nopCompiler,                  // SQL query compiler.

      host           : config.host || null,          // SQL server host.
      port           : config.port || null,          // SQL server port.
      username       : config.username || null,      // SQL user name.
      password       : config.password || null,      // SQL user's password.
      database       : config.database || null,      // SQL database.

      debugQueries   : Boolean(config.debugQueries), // Print queries.
      debugResults   : Boolean(config.debugResults), // Print queries.

      clientsCount   : 0,                            // Number of all clients.
      clientsActive  : 0,                            // Number of active clients.
      clientsMinimum : config.minConnections || 0,   // Minimum number of clients.
      clientsMaximum : config.maxConnections || 20,  // Maximum number of clients.

      failuresCount  : 0,                            // Count of failure attempts to create a client.
      failuresMaximum: config.maximumFailures || 20, // Maximum of failure attempts to create the first client.

      clientPool     : null,                         // SQL client pool.
      queueFirst     : null,                         // First item in work queue (FIFO).
      queueLast      : null,                         // Last item in work queue (FIFO).
      queueSize      : 0,                            // Number of items in the queue.

      txIdGenerator  : 0,                            // Transaction ID counter.
      onClientQuery  : onClientQuery,                // Query handler callback.
      onClientCreated: onClientCreated,              // Client idle callback.
      onDelayedStop  : null                          // Delayed stop callback.
    };

    // Internal callbacks, bound only once to decrease the memory footprint.
    function onClientQuery(err, result) {
      self._onClientQuery(err, result);
    }

    function onClientCreated(err, client) {
      self._onClientCreated(err, client);
    }
  }

  /**
   * Configure the driver based on the configuration. Always called by the class
   * that extends the driver.
   */
  _postConfigure(config, defaults) {
    const internal = this._internal;
    const dialect = internal.dialect;

    // This is gonna throw, as intended, if there is no package that provides
    // the driver. The reason  is that we cannot recover from this. The user
    // can pass `config.backend` if different driver should be used.
    if (internal.impl === null)
      internal.impl = require(defaults.driver);

    if (config.compiler) {
      switch (config.compiler) {
        case "xql":
          internal.compiler = require("xql").dialect.newContext({ dialect: dialect });
          break;

        default:
          throw SQLError("Unrecognized SQL compiler");
      }
    }
  }

  /**
   * Implements `xpart.start` interface.
   *
   * Performs basic checks and calls `_start()`, which can be overridden by the driver.
   *
   * @param {function} cb Start callback.
   */
  start(cb) {
    const internal = this._internal;

    if (internal.status !== "pending") {
      setImmediate(cb, new SQLError("The SQL driver has been already started (driver status: '" + internal.status + "')"), null);
      return;
    }

    this._start(cb);
  }

  /**
   * Implements `xpart.stop` interface.
   *
   * Performs basic checks and calls `_stop()`, which can be overridden by the
   * driver, after all operations finish execution. After `stop()` is called
   * the driver will refuse all future SQL requests, but completes all ongoing.
   *
   * @param {function} cb Stop callback.
   */
  stop(cb) {
    const internal = this._internal;

    if (internal.status !== "running" || internal.onDelayedStop) {
      setImmediate(cb, new SQLError("The SQL driver has been already stopped (driver status: '" + internal.status + "')"), null);
      return;
    }

    internal.status = "stopping";

    if (internal.clientsActive !== 0) {
      internal._onDelayedStop = cb;
    }
    else {
      // Can only stop if there are no active clients at the moment. Otherwise the
      // delayed stop is used. Now new queries or transactions will be allowed.
      this._destroyPool();
      this._stop(cb);
    }
  }

  /**
   * Returns the status of the driver.
   *
   * @return {string}
   *   Status of the driver:
   *     - `"pending"`  - The driver haven't started yet - `start()` has not
   *       been called.
   *     - `"starting"` - The driver is starting at the moment - `start()` has
   *       been called, but didn't callback yet.
   *     - `"running"`  - The driver is running at the moment.
   *     - `"stopping"` - The driver is terminating at the moment - `stop()`
   *       has been called, but didn't callback yet.
   *     - `"stopped"`  - The driver is stopped.
   */
  getStatus() {
    return this._internal.status;
  }

  /**
   * Returns the SQL driver's dialect:
   *
   *   - `"mysql"` if the driver is `mysql`.
   *   - `"pgsql"` if the driver is `pg`.
   *   - `"sqlite"` if the driver is `sqlite`.
   *
   * @return {string}
   */
  getDialect() {
    return this._internal.dialect;
  }

  /**
   * Performs a SQL query.
   *
   * @param {*} q Query string or a query-builder object.
   * @param {function} cb Query callback.
   * @param {object} [tx] Transaction.
   */
  query(q, cb, tx) {
    const internal = this._internal;
    const qs = internal.compiler.compile(q);

    if (tx)
      return tx.query(qs, cb);

    // Check whether the driver is running. It's an application failure if it
    // calls `query()` while the driver didn't start yet or after it started
    // shutting down.
    if (internal.status !== "running") {
      setImmediate(cb, new SQLError("The SQL driver cannot perform the query (driver status: '" + internal.status + "')"), null);
      return;
    }

    if (internal.clientPool)
      this._getClientFromPool().query(qs, cb);
    else
      this._addToQueue(qs, cb);
  }

  /**
   * Begins a new SQL transaction and passes the transaction object to the
   * `cb` callback. The created transaction has to be finalized by either
   * calling `commit()`, `rollback()`, or `cancel()`. See `SQLClient` for more
   * details.
   *
   * @param {function} cb Called when the transaction object is ready.
   */
  begin(cb) {
    const internal = this._internal;

    // Check whether the driver is running. It's an application failure if it
    // calls `beginTransaction()` while the driver didn't start yet or after it
    // started shutting down. Since this is async we just pass the error to the
    // callback.
    if (internal.status !== "running") {
      setImmediate(cb, new SQLError("The SQL driver cannot begin a new transaction (driver status: '" + internal.status + "')"), null);
      return;
    }

    if (internal.clientPool)
      this._getClientFromPool().begin(cb);
    else
      this._addToQueue(null, cb);
  }

  /**
   * Start callback.
   *
   * @param {function} cb Callback to call.
   * @private
   */
  _start(cb) {
    const internal = this._internal;
    internal.status = "running";
    setImmediate(cb, null);
  }

  /**
   * Stop callback.
   *
   * @param {function} cb Callback to call.
   * @private
   */
  _stop(cb) {
    const internal = this._internal;
    internal.status = "stopped";
    internal.onDelayedStop = null;
    setImmediate(cb, null);
  }

  /**
   * Adds a `query` to the internal queue to be handled later.
   *
   * NOTE: If the query is `null` it's a transaction, which is handled a bit
   * differently.
   *
   * @param {*} qs Query to add to the queue (or `null` if it's a transaction).
   * @param {function} cb Callback to call when the query can be executed.
   *
   * @private
   */
  _addToQueue(qs, cb) {
    const internal = this._internal;
    const last = internal.queueLast;

    const item = {
      qs: qs,
      cb: cb,
      next: null
    };

    if (last !== null)
      last.next = item;
    else
      internal.queueFirst = item;

    internal.queueLast = item;
    internal.queueSize++;

    // Inform the driver that some work has been added to the queue.
    this._scheduleWork();
  }

  /**
   * Handle one item in the internal query queue. The caller has to check
   * whether there is at least one item before calling `_handleQueue()`.
   *
   * @param {SQLClient} client SQL client that will be used to handle the query
   *   or transaction in the queue.
   *
   * @private
   */
  _handleQueue(client) {
    const internal = this._internal;

    const item = internal.queueFirst;
    const next = item.next;

    if (next === null)
      internal.queueLast = null;

    internal.queueFirst = next;
    internal.queueSize--;

    const qs = item.qs;
    const cb = item.cb;

    if (qs !== null)
      client.query(qs, cb);
    else
      client.begin(cb);
  }

  /**
   * Puts the driver into an idle state. Idle state means that there are no
   * more items in the internal queue and there are no active SQL clients.
   *
   * Idle is called mostly to gratefully stop the SQL driver when no more
   * requests are pending. It has generally no effect on a running application.
   *
   * @private
   */
  _idle() {
    const internal = this._internal;

    // If the driver is stopping and idle it's the right time to gratefully
    // stop the module by calling `_stop()`. Could be delayed stop as well.
    if (internal.clientsActive === 0 && internal.status === "stopping") {
      this._destroyPool();
      this._stop(internal.onDelayedStop);
    }
  }

  /**
   * Checks whether the work queue is not empty and assigns a client to the
   * first item in the queue. It will try to create a new client if the
   * number of created clients didn't exceed the limit. Called after the work
   * has been added to the queue or after a client failed to instantiate.
   *
   * @private
   */
  _scheduleWork() {
    const internal = this._internal;

    if (internal.queueFirst) {
      if (internal.clientPool) {
        this._handleQueue(this._getClientFromPool());
        return;
      }

      // Create a new client if the number of clients didn't exceed the limit.
      if (internal.clientsCount < internal.clientsMaximum) {
        internal.clientsCount++;
        this._createClient(internal.onClientCreated);
      }
    }
  }

  /**
   * Called after a new `client` has been created.
   *
   * @param {?Error} err Error that happened when creating the SQLClient.
   * @param {?SQLClient} client A new SQLClient instance.
   *
   * @private
   */
  _onClientCreated(err, client) {
    const internal = this._internal;

    if (client) {
      internal.clientsActive++;
      return this._onClientIdle(client);
    }

    this.app.error("Failed to create a new SQLClient: ", err.toString());

    // The driver increments `clientCount` before it calls `_CreateClient()`,
    // to ensure that it doesn't create more clients than `clientsMaximum`.
    // If the driver failed to create the client it has to be decremented as
    // well, otherwise it will end up in an inconsistent state.
    internal.clientsCount--;
    internal.failuresCount++;

    // If the driver failed to create the first client there is probably
    // some problem with configuration or infrastructure. The driver will
    // keep trying up to `failuresMaximum` attempts, after that it will
    // report the error and call the failure handler.
    if (internal.clientsCount === 0) {
      if (internal.failuresCount >= internal.failuresMaximum) {
        // TODO:
      }
      else {
        this._scheduleWork();
      }
    }
  }

  /**
   * Called when a `client` became idle.
   *
   * The given client will handle the next item in the internal queue or will
   * go idle.
   *
   * @param {SQLClient} client SQL client that can go idle.
   *
   * @private
   */
  _onClientIdle(client) {
    const internal = this._internal;

    if (internal.queueFirst)
      this._handleQueue(client);
    else
      this._releaseClient(client);
  }

  /**
   * Called to get a SQL client from the pool. It does the necessary work
   * to unlink the client from the pool and to update internal counters.
   *
   * NOTE: There must be a client in the pool, check `internal.clientPool`
   * before calling this function.
   *
   * @private
   */
  _getClientFromPool() {
    const internal = this._internal;
    const client = internal.clientPool;

    internal.clientPool = client._next;
    internal.clientsActive++;

    client._pooled = false;
    client._next = null;

    return client;
  }

  /**
   * Releases the SQL client to the client pool or destroys it.
   *
   * @param {SQLClient} client SQL client to release or destroy.
   *
   * @private
   */
  _releaseClient(client) {
    const internal = this._internal;
    const keep = internal.status === "running" && !client._failed;

    if (!keep) {
      // OOOPS! If the client failed or is in an inconsistent state it's
      // always better to just destroy it and create a new one later. This
      // is recommended by the most DB drivers anyway. Also, if we are
      // shutting down we don't pool released clients as well.
      internal.clientsCount--;
      internal.clientsActive--;

      this._destroyClient(client);
    }
    else {
      client._next = internal.clientPool;
      client._txId = -1;
      client._txState = "";
      client._pooled = true;
      client._returnToPool = true;

      internal.clientsActive--;
      internal.clientPool = client;
    }

    this._idle();
  }

  /**
   * Releases all idle clients waiting in the client-pool.
   *
   * @private
   */
  _destroyPool() {
    const internal = this._internal;

    var client = internal.clientPool;
    var n = 0;

    while (client) {
      var next = client._next;
      this._destroyClient(client);

      client = next;
      n++;
    }

    internal.clientsCount -= n;
    internal.clientsActive -= n;
    internal.clientPool = null;
  }

  /**
   * Generates a unique transaction-id.
   *
   * @private
   */
  _newTxId() {
    const internal = this._internal;
    return ++internal.txIdGenerator;
  }

  /**
   * Gets the URI of the server to connect to based on the configuration passed
   * to the `SQLServer()` constructor. Some implementations (SQLite) will never
   * call `_getServerURL()` as they use a local file system.
   *
   * @private
   */
  _getServerURL() {
    throw new TypeError("SQLDriver._getServerURL() is abstract");
  }

  /**
   * Creates a new SQL client.
   *
   * @private
   */
  _createClient(cb) {
    throw new TypeError("SQLDriver._createClient() is abstract");
  }

  /**
   * Destroys the SQL client.
   *
   * @private
   */
  _destroyClient(client) {
    throw new TypeError("SQLDriver._destroyClient() is abstract");
  }
}
exports.SQLDriver = SQLDriver;

// ============================================================================
// [SQLClient]
// ============================================================================

/**
 * A wrapper around a native SQL client / connection, used by `SQLDriver`.
 */
class SQLClient {
  constructor(driver, impl) {
    this._driver = driver;     // SQL driver (owner).
    this._impl = impl;         // SQL client (implementation).
    this._next = null;         // Next client in the client pool.

    this._txId = -1;           // Transaction ID, -1 if not transacting.
    this._txState = "";        // Transaction state (internal to SQLClient).

    this._failed = false;      // True if the client failed a transaction.
    this._pooled = false;      // True if the client is in the client pool now.
    this._returnToPool = true; // Return to the connection pool after query.

    this._qs = "";             // Query string (stored for better error reports).
    this._cb = null;           // Query callback (if performing query now).
    this._onQuery = null;      // Query handler, has to be implemented by the driver.
  }

  begin(cb) {
    const driver = this._driver;

    if (this._txId !== -1) {
      setImmediate(cb, null, new SQLError("Cannot BEGIN while being in a transaction state {txId=" + this._txId + "}"));
      return driver._onClientIdle(this);
    }

    // Set client to a transaction mode.
    this._txId = driver._newTxId();
    this._txState = "";
    this._returnToPool = false;

    // Begin has to be async.
    setImmediate(cb, null, this);
  }

  commit(/* [q,] */ cb) {
    const driver = this._driver;
    var q = "";

    if (arguments.length > 1) {
      q = arguments[0];
      cb = arguments[1];
    }

    var qs = driver._internal.compiler.compile(q);

    if (this._txId === -1) {
      setImmediate(cb, null, new SQLError("Cannot COMMIT while not being in a transaction state"));
      return driver._onClientIdle(this);
    }

    if (this._txState === "") {
      if (!qs) {
        // Do nothing if the transaction is empty.
        setImmediate(cb, null, null);
        return driver._onClientIdle(this);
      }

      qs = "BEGIN;\n" + qs;
    }

    if (!qs)
      qs = "COMMIT;";
    else
      qs += "\nCOMMIT;";

    this._txState = "COMMIT";
    this._returnToPool = true;
    return this.query(qs, cb);
  }

  rollback(cb) {
    const driver = this._driver;

    if (this._txId === -1) {
      setImmediate(cb, null, new SQLError("Cannot ROLLBACK while not being in a transaction state"));
      return driver._onClientIdle(this);
    }

    // Do nothing if the transaction is empty.
    if (this._txState === "") {
      setImmediate(cb, null, null);
      return driver._onClientIdle(this);
    }

    var qs = "ROLLBACK;";
    this._txState = "ROLLBACK";
    this._returnToPool = true;
    return this.query(qs, cb);
  }

  query(q, cb) {
    const driver = this._driver;
    var qs = driver._internal.compiler.compile(q);

    // If this is a transacting query make sure that the transaction is
    // started by "BEGIN", the SQLClient won't do that before the first
    // query is about being executed.
    if (this._txId !== -1) {
      if (this._txState === "") {
        qs = "BEGIN;\n" + qs;
        this._txState = "PENDING";
      }
    }

    this._query(qs, cb);
  }

  _query(qs, cb) {
    throw new TypeError("SQLClient._query() is abstract");
  }
}
exports.SQLClient = SQLClient;
