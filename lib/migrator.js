'use strict';

var fs = require('fs')
  , moment = require('moment')
  , async = require('async');

var Utils = require(__dirname + '/utils')
  , Migration = require(__dirname + '/migration')
  , DataTypes = require(__dirname + '/data-types');

module.exports = (function() {
  var Migrator = function(sequelize, options) {
    this.sequelize = sequelize;
    this.options = Utils._.extend({
      path: __dirname + '/../migrations',
      from: null,
      to: null,
      logging: this.sequelize.log.bind(this.sequelize),
      filesFilter: /\.js$/
    }, options || {});
  };

  Object.defineProperty(Migrator.prototype, 'queryInterface', {
    get: function() {
      return this.sequelize.getQueryInterface();
    }
  });

  Migrator.prototype.create = function(callback) {

  };

  Migrator.prototype.migrate = function(options, callback) {
    var self = this;
    var dao;

    async.waterfall([
      // get dao
      function(cb) {
        self.findOrCreateSequelizeMetaDAO()
          .complete(cb);
      },
      // setup migrator
      function(newDao, cb) {
        dao = newDao;
        // get file lists
        var migrationFiles = fs.readdirSync(self.options.path).filter(function(file) {
          return self.options.filesFilter.test(file);
        });

        var migrations = migrationFiles.map(function(file) {
          return new Migration(self, self.options.path + '/' + file);
        });

        self.sequelize

        // *TK if migrations.length > 0 and sequelizeMeta table empty do this, else do nothing
        async.each(migrations, function(mig, cb2) {
          dao.create({
            id: mig.migrationId,
            filename: mig.filename,
            current: false
          }).complete(function(err, something) {
            // currently ignoring failures
            cb2();
          });
        }, function(err) {
          cb();
        });
      },
      // get new migrations
      function(cb) {
        dao.getNewMigrations(options.method, options.times, cb);
      },
      // process migrations
      function(migs, cb) {
        self.processMigrations(migs, options.method, cb);
      }
      // final callback
    ], callback);
  };


  Migrator.prototype.processMigrations = function(migrations, method, callback) {
    var self = this;

    var migFileNames = migrations.map(function(mig) {
      return self.options.path + '/' + mig.filename;
    });

    self.exec(migFileNames, method, callback);
  };

  Migrator.prototype.findOrCreateSequelizeMetaDAO = function(syncOptions) {
    var self = this;

    return new Utils.CustomEventEmitter(function(emitter) {
      var storedDAO = self.sequelize.daoFactoryManager.getDAO('SequelizeMeta')
        , SequelizeMeta = storedDAO;

      if (!storedDAO) {
        SequelizeMeta = self.sequelize.define('SequelizeMeta', {
          id: {
            type: DataTypes.DATE,
            primaryKey: true
          },
          filename: DataTypes.STRING,
          current: DataTypes.BOOLEAN,
          state: DataTypes.STRING
        }, {
          timestamps: false,
          classMethods: {
            getNewMigrations: function(method, times, callback) {
              var innerSelf = this;

              async.waterfall([
                // get index and state of current migration
                function(cb) {
                  var qString1 = 'SELECT id, state FROM "SequelizeMeta" WHERE "current" IS true LIMIT 1';
                  self.sequelize.query(qString1, innerSelf, {raw: true}).complete(function(err, result) {
                    // error
                    if(err) {
                      cb(err);
                    }
                    // db is empty OR it is an initial migration
                    else if(result.length == 0) {
                      cb(null, null);
                    }
                    // found an index
                    else {
                      cb(null, result[0]);
                    }
                  });
                },
                // get all migrations before or after
                function(index, cb) {
                  // if no cb, then there is no index
                  if(!cb) {
                    cb = index;
                    index = null;
                  }
                  var order;
                  var qString2;

                  if(method === "down") {
                    order = 'DESC';
                  }
                  else if(method === "up") {
                    order = 'ASC';
                  }
                  
                  // no index, so run them all
                  if(!index) {
                    qString2 = 'SELECT * FROM "SequelizeMeta" ORDER BY "id" '+order;
                  }
                  // if there was an index, we need to do a bunch more shit
                  else {
                    var operator;
                    var limitString = '';
                    var sqlDate = JSON.stringify(index.id);
                    
                    // if state of index is UP, and we're going DOWN, we need to return all <= index
                    if(index.state === "up" && method === "down" ) {
                      operator = '<=';
                    }
                    // if state of index is UP, and we're going UP, we need to return all > index
                    else if(index.state === "up" && method === "up") {
                      operator = '>';
                    }
                    // if state of index is DOWN, and we're going DOWN, we need to return all < index
                    else if(index.state === "down" && method === "down") {
                      operator = '<';
                    }
                    // if state of index is DOWN, and we're going UP, we need to return all >= index
                    else if(index.state === "down" && method === "up") {
                      operator = '>=';
                    }
                    // if a number of migrations to run was passed, set the limit here
                    if(times !== 'all') {
                      limitString = ' LIMIT '+times;
                    }

                    qString2 = 'SELECT * FROM "SequelizeMeta" WHERE "id" '+operator+' \''+sqlDate+'\' ORDER BY "id" '+order+limitString;
                  }
                  self.sequelize.query(qString2, innerSelf, {raw: true}).complete(function(err, result) {
                    if(result) {
                      return callback(null, result);
                    }
                    else {
                      cb(err);
                    }
                  });
                }
              ], function(err) {
                
                callback(err);
              });
            },
          }
        });
      }

      // force sync when model has newly created or if syncOptions are passed
      if (!storedDAO || syncOptions) {
        SequelizeMeta
          .sync(syncOptions || {})
          .success(function() { emitter.emit('success', SequelizeMeta); })
          .error(function(err) { emitter.emit('error', err); });
      } else {
        emitter.emit('success', SequelizeMeta);
      }
    }).run();
  };

  // needs method, migrationId,
  Migrator.prototype.moveCursor = function(id, method, callback) {
    var self = this;
    var dao;
    // get current cursor
    // move to new migId
    // callback
    async.waterfall([
      // get dao
      function(cb) {
        self.findOrCreateSequelizeMetaDAO()
          .complete(cb);
      },
      // falsify last migration
      function(newDao, cb) {
        dao = newDao;
        self.sequelize.query('UPDATE "SequelizeMeta" SET "current"=false WHERE "current"=true').complete(function(err, result) {
          cb();
        });
      },
      // set current migration
      function(cb) {
        var qString = 'UPDATE "SequelizeMeta" SET "current"=true, "state"=:param1 WHERE "id"=:param2';
        var qParams = {
          param1: method,
          param2: id
        };
        self.sequelize.query(qString, dao, {}, qParams).complete(function(err, result) {
          cb();
        });
      }
    ], callback);
  };

    /**
   * Explicitly executes one or multiple migrations.
   *
   * @param filename {String|Array} Absolute filename(s) of the migrations script
   * @param options  {Object}       Can contain three functions, before, after and success, which are executed before
   *                                or after each migration respectively, with one parameter, the migration.
   */
  Migrator.prototype.exec = function(filename, method, callback) {
    var self = this;
    var chainer = new Utils.QueryChainer();
    
    var addMigration = function(filename) {
      var migration = new Migration(self, filename);

      chainer.add(migration, 'execute', [{ method: method }], {
        // after each migration, move cursor
        before: function(migration, cb) {
          self.options.logging('Running migration: ', filename);
        },
        success: function(migration, cb) {
          self.options.logging('Migration success: ', filename);
          self.moveCursor(new Date(migration.migrationId), method, cb);
        }
      });
    };

    if (Utils._.isArray(filename)) {
      Utils._.each(filename, function(f) {
        addMigration(f);
      });
    } else {
      addMigration(filename);
    }

    chainer
      .runSerially({ skipOnError: true })
      .complete(callback);
  };

  return Migrator;
})();