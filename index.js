"use strict";

var exclass = require("exclass");
var hasOwn = Object.prototype.hasOwnProperty;

// ============================================================================
// [Module (exapp.js)]
// ============================================================================

function new_(app, config) {
  var engine = getEngine(config.engine);
  return engine.new(app, config);
}
exports["new"] = new_;

// ============================================================================
// [Utils]
// ============================================================================

// Get the SQL engine by `name`.
//
// It throws if the SQL module
function getEngine(name) {
  if (!/[a-z_][a-z0-9_]*/.test(name))
    throw new Error("Invalid module '" + name + "'");

  return require("./engine-" + name + ".js");
}
exports.getEngine = getEngine;

function flattenQuery(q, options) {
  // Support UQL and compatible query builders.
  if (typeof q.compileQuery === "function")
    return q.compileQuery(/* TODO: UQL options */);
  else
    return q.toString();
}
exports.flattenQuery = flattenQuery;

// ============================================================================
// [SQLError]
// ============================================================================

// \class SQLError
//
// SQL error that can be returned by an underlying SQL driver.
function SQLError(msg) {
  this.name = "SQLError";
  this.message = msg;
  this.stack = Error(msg).stack || "";

  // HACK: Make sure the `SQLError` is reported instead of `Error`.
  if (this.stack)
    this.stack = this.name + this.stack.substr(5);
}
exports.SQLError = exclass({
  $extend: Error,
  $construct: SQLError
});

// ============================================================================
// [SQLDriver]
// ============================================================================

// \class SQLDriver
//
// Base class that implements functionality required by all SQL drivers.
function SQLDriver(app, config) {
  var self = this;

  // Internals are always used by driver implementations.
  this.app = app;
  this._internal = {
    impl           : null,                         // SQL driver (pg, mysql, etc).
    status         : "pending",                    // SQL driver status.

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
exports.SQLDriver = exclass({
  $construct: SQLDriver,

  // Implements `exapp.js` start interface.
  //
  // Performs basic checks and calls `_start()`, which can be overridden by the driver.
  start: function(cb) {
    var internal = this._internal;

    if (internal.status !== "pending") {
      setImmediate(cb, new SQLError("The SQL driver has been already started (driver status: '" + internal.status + "')"), null);
      return;
    }

    this._start(cb);
  },

  // Implements `exapp.js` stop interface.
  //
  // Performs basic checks and calls `_stop()`, which can be overridden by the
  // driver, after all operations finish execution. After `stop()` is called
  // the driver will refuse all future SQL requests, but completes all ongoing.
  stop: function(cb) {
    var internal = this._internal;

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
  },

  // Returns the status of the driver.
  //
  // The possible return values are:
  //   - "pending"  - The driver didn't start yet - `start()` has not been called.
  //   - "starting" - The driver is starting at the moment - `start()` has been
  //                  called, but didn't callback yet.
  //   - "running"  - The driver is running at the moment.
  //   - "stopping" - The driver is terminating at the moment - `stop()` has been
  //                  called, but didn't callback yet.
  //   - "stopped"  - The driver is stopped.
  getStatus: function() {
    return this._internal.status;
  },

  // \function `SQLDriver.query(q, cb [, tx])`
  //
  // Perform a SQL query.
  query: function(q, cb, tx) {
    var internal = this._internal;
    var qs = flattenQuery(q);

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
  },

  // \function `SQLDriver.transaction(cb)`
  //
  // Begins a new transaction object and passes it to the callback `cb`. The
  // created transaction has to be finalized by either calling `commit()`,
  // `rollback()`, or `cancel()`. See `SQLClient` for more details.
  begin: function(cb) {
    var internal = this._internal;

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
  },

  // \internal
  //
  // Start callback.
  _start: function(cb) {
    var internal = this._internal;
    internal.status = "running";
    setImmediate(cb, null);
  },

  // \internal
  //
  // Stop callback.
  _stop: function(cb) {
    var internal = this._internal;
    internal.status = "stopped";
    internal.onDelayedStop = null;
    setImmediate(cb, null);
  },

  // Adds a `query` to the internal queue to be handled later.
  //
  // NOTE: If the query is `null` it's a transaction (handled differently).
  _addToQueue: function(qs, cb) {
    var internal = this._internal;
    var last = internal.queueLast;

    var item = {
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
  },

  // The caller has to check whether there is at least one item before
  // calling `_handleQueue()`.
  _handleQueue: function(client) {
    var internal = this._internal;

    var item = internal.queueFirst;
    var next = item.next;

    if (next === null)
      internal.queueLast = null;

    internal.queueFirst = next;
    internal.queueSize--;

    var qs = item.qs;
    var cb = item.cb;

    if (qs !== null)
      client.query(qs, cb);
    else
      client.begin(cb);
  },

  _idle: function() {
    var internal = this._internal;

    // If the driver is stopping and idle it's the right time to gratefully
    // stop the module by calling `_stop()`. Could be delayed stop as well.
    if (internal.clientsActive === 0 && internal.status === "stopping") {
      this._destroyPool();
      this._stop(internal.onDelayedStop);
    }
  },

  // Checks whether the work queue is not empty and assigns a client to the
  // first item in the queue. It will try to create a new client if the
  // number of created clients didn't exceed the limit. Called after the work
  // has been added to the queue or after a client failed to instantiate.
  _scheduleWork: function() {
    var internal = this._internal;

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
  },

  // Called after a new `client` has been created.
  _onClientCreated: function(err, client) {
    var self = this;
    var internal = this._internal;

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
  },

  // Called when a `client` became idle.
  _onClientIdle: function(client) {
    var internal = this._internal;

    if (internal.queueFirst)
      this._handleQueue(client);
    else
      this._releaseClient(client);
  },

  // Called to get a SQL client from the pool. It does the necessary work
  // to unlink the client from the pool and to update internal counters.
  //
  // NOTE: There must be a client in the pool, check `internal.clientPool`
  // before calling this function.
  _getClientFromPool: function() {
    var internal = this._internal;
    var client = internal.clientPool;

    internal.clientPool = client._next;
    internal.clientsActive++;

    client._pooled = false;
    client._next = null;

    return client;
  },

  // Releases the SQL client to the client pool or destroys it.
  _releaseClient: function(client) {
    var internal = this._internal;
    var keep = internal.status === "running" && !client._failed;

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
  },

  // Releases all pooled clients.
  _destroyPool: function() {
    var internal = this._internal;
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
  },

  _newTxId: function() {
    var internal = this._internal;
    return ++internal.txIdGenerator;
  },

  // \internal
  //
  // Gets the URI of the server to connect to based on the configuration passed
  // to the `SQLServer()` constructor. Some implementations (SQLite) will never
  // call `_getServerURL()` as they use a local file system.
  _getServerURL: function() {
    throw new TypeError("SQLDriver._getServerURL() is abstract");
  },

  // \internal
  //
  // Creates a new SQL client.
  _createClient: function(cb) {
    throw new TypeError("SQLDriver._createClient() is abstract");
  },

  // \internal
  //
  // Destroys the SQL client.
  _destroyClient: function(client) {
    throw new TypeError("SQLDriver._destroyClient() is abstract");
  }
});

// ============================================================================
// [SQLClient]
// ============================================================================

// \class SQLClient
//
// A wrapper around a native SQL client / connection, used by `SQLDriver`.
function SQLClient(driver, impl) {
  var self = this;

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
exports.SQLClient = exclass({
  $construct: SQLClient,

  begin: function(cb) {
    var driver = this._driver;

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
  },

  commit: function(/* [q,] */ cb) {
    var q = "";

    if (arguments.length > 1) {
      q = arguments[0];
      cb = arguments[1];
    }

    var driver = this._driver;
    var qs = flattenQuery(q);
    
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
  },

  rollback: function(cb) {
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
  },

  query: function(q, cb) {
    var qs = flattenQuery(q);

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
  },

  _query: function(qs, cb) {
    throw new TypeError("SQLClient._query() is abstract");
  }
});
