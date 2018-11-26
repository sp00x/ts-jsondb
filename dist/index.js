"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var FS = require("fs");
var Path = require("path");
var Util = require("util");
var uuid_1 = require("uuid");
var ObjectPath = require("object-path");
var LockFile = require("lockfile");
var log_interface_1 = require("@sp00x/log-interface");
var mkdirp = Util.promisify(require('mkdirp'));
var writeFile = Util.promisify(FS.writeFile);
var readFile = Util.promisify(FS.readFile);
var readDir = Util.promisify(FS.readdir);
var deleteFile = Util.promisify(FS.unlink);
var lockFile = Util.promisify(LockFile.lock);
var unlockFile = Util.promisify(LockFile.unlock);
var DEFAULT_ID_PROPERTY_NAME = '_id';
var LOCK_FILE_EXT = '.lock';
var DATA_FILE_EXT = '.json';
var Database = (function () {
    function Database(path, log) {
        this.collections = {};
        this.isInitialized = false;
        this.log = log != null ? log : new log_interface_1.NullLogger();
        this.path = path;
    }
    Database.prototype.initialize = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!!this.isInitialized) return [3, 2];
                        return [4, mkdirp(this.path)];
                    case 1:
                        _a.sent();
                        this.isInitialized = true;
                        _a.label = 2;
                    case 2: return [2];
                }
            });
        });
    };
    Database.prototype.collection = function (name, options) {
        if (options === void 0) { options = {}; }
        return __awaiter(this, void 0, void 0, function () {
            var c;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4, this.initialize()];
                    case 1:
                        _a.sent();
                        c = this.collections[name];
                        if (c != null)
                            return [2, c];
                        c = new Collection(this, name, options);
                        this.collections[name] = c;
                        return [2, c];
                }
            });
        });
    };
    return Database;
}());
exports.Database = Database;
var ObjectID = (function () {
    function ObjectID(value) {
        this.value = (value != null) ? value : uuid_1.v4();
    }
    ObjectID.prototype.toString = function () {
        return this.value;
    };
    return ObjectID;
}());
exports.ObjectID = ObjectID;
function clone(obj) {
    return JSON.parse(JSON.stringify(obj));
}
var Collection = (function () {
    function Collection(db, name, options) {
        if (options === void 0) { options = {}; }
        this.isInitialized = false;
        this.cache = {};
        this.db = db;
        this.name = name;
        this.log = db.log instanceof log_interface_1.NullLogger ? db.log : new log_interface_1.PrefixedLogger("<" + name + "> ", db.log);
        this.options = options;
        this.idPropertyName = (typeof this.options.idPropertyName == 'string') ? this.options.idPropertyName : DEFAULT_ID_PROPERTY_NAME;
    }
    Object.defineProperty(Collection.prototype, "isCacheEnabled", {
        get: function () {
            return this.options.cache === true;
        },
        enumerable: true,
        configurable: true
    });
    Collection.prototype.initialize = function () {
        return __awaiter(this, void 0, void 0, function () {
            var files, _i, files_1, fn, id;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!!this.isInitialized) return [3, 7];
                        this.path = Path.join(this.db.path, this.name);
                        return [4, mkdirp(this.path)];
                    case 1:
                        _a.sent();
                        if (!this.options.cache) return [3, 6];
                        return [4, this.getAllDocFilenames()];
                    case 2:
                        files = _a.sent();
                        _i = 0, files_1 = files;
                        _a.label = 3;
                    case 3:
                        if (!(_i < files_1.length)) return [3, 6];
                        fn = files_1[_i];
                        id = this.getIdFromFilename(fn);
                        return [4, this.readDoc(id)];
                    case 4:
                        _a.sent();
                        _a.label = 5;
                    case 5:
                        _i++;
                        return [3, 3];
                    case 6:
                        this.isInitialized = true;
                        _a.label = 7;
                    case 7: return [2];
                }
            });
        });
    };
    Collection.prototype.makeFullPath = function (id, version) {
        return (version == undefined)
            ? Path.join(this.path, id.toString()) + '.json'
            : Path.join(this.path, id.toString()) + '.' + version + '.json';
    };
    Collection.prototype.ensureId = function (doc) {
        var id = (doc[this.idPropertyName] == null) ? uuid_1.v4() : doc[this.idPropertyName].toString();
        doc[this.idPropertyName] = id;
        return id;
    };
    Collection.prototype.save = function (doc) {
        return __awaiter(this, void 0, void 0, function () {
            var log, ins, id;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        log = this.log;
                        return [4, this.initialize()];
                    case 1:
                        _a.sent();
                        if (doc == null || typeof doc != 'object')
                            throw new Error("Not an object");
                        ins = __assign({}, doc);
                        id = this.ensureId(ins);
                        log.debug("saving: %s", id);
                        return [4, this.writeDoc(ins)];
                    case 2:
                        _a.sent();
                        return [2, {
                                numAffected: 1,
                                numInserted: 1,
                                numDeleted: 0,
                                numUpdated: 0,
                                docs: [ins]
                            }];
                }
            });
        });
    };
    Collection.prototype.find = function (query, options) {
        if (options === void 0) { options = {}; }
        return __awaiter(this, void 0, void 0, function () {
            var log, docs;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        log = this.log;
                        query = this.preprocessQuery(query);
                        docs = [];
                        return [4, this.allDocs(function (doc) { return __awaiter(_this, void 0, void 0, function () { return __generator(this, function (_a) {
                                return [2, docs.push(doc)];
                            }); }); }, query, options)];
                    case 1:
                        _a.sent();
                        return [2, docs];
                }
            });
        });
    };
    Collection.prototype.findOne = function (query, options) {
        if (options === void 0) { options = {}; }
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4, this.find(query, __assign({ limit: 1 }, options))];
                    case 1: return [2, (_a.sent())[0]];
                }
            });
        });
    };
    Collection.prototype.delete = function (query, options) {
        if (options === void 0) { options = {}; }
        return __awaiter(this, void 0, void 0, function () {
            var log, summary, docs;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        log = this.log;
                        return [4, this.initialize()];
                    case 1:
                        _a.sent();
                        query = this.preprocessQuery(query);
                        log.debug("delete where %j", query);
                        summary = {
                            numAffected: 0,
                            numDeleted: 0,
                            numUpdated: 0,
                            numInserted: 0
                        };
                        docs = [];
                        return [4, this.allDocs(function (doc) { return __awaiter(_this, void 0, void 0, function () {
                                var fn, id, e_1;
                                return __generator(this, function (_a) {
                                    switch (_a.label) {
                                        case 0:
                                            log.debug("delete callback: %j", doc);
                                            fn = this.makeFullPath(doc[this.idPropertyName]);
                                            _a.label = 1;
                                        case 1:
                                            _a.trys.push([1, 3, , 4]);
                                            log.debug("deleting: %s", fn);
                                            return [4, deleteFile(fn)];
                                        case 2:
                                            _a.sent();
                                            if (this.isCacheEnabled) {
                                                id = doc[this.idPropertyName];
                                                delete this.cache[id];
                                            }
                                            summary.numAffected++;
                                            summary.numDeleted++;
                                            return [3, 4];
                                        case 3:
                                            e_1 = _a.sent();
                                            log.error("Error deleting file: %s - %s (%s)", fn, e_1.message, e_1.code);
                                            return [3, 4];
                                        case 4: return [2];
                                    }
                                });
                            }); }, query, options)];
                    case 2:
                        _a.sent();
                        return [2, summary];
                }
            });
        });
    };
    Collection.prototype.updateOne = function (query, update, options) {
        if (options === void 0) { options = {}; }
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4, this.update(query, update, __assign({}, options, { limit: 1 }))];
                    case 1: return [2, _a.sent()];
                }
            });
        });
    };
    Collection.prototype.update = function (query, update, options) {
        if (options === void 0) { options = {}; }
        return __awaiter(this, void 0, void 0, function () {
            var log, summary, keys, ops, hasOps;
            var _this = this;
            return __generator(this, function (_a) {
                log = this.log;
                summary = {
                    docs: [],
                    numAffected: 0,
                    numDeleted: 0,
                    numInserted: 0,
                    numUpdated: 0
                };
                keys = Object.keys(update);
                ops = keys.filter(function (k) { return /^\$/.test(k); });
                hasOps = ops.length > 0;
                if (hasOps && ops.length != keys.length)
                    throw new Error("Update document can not be a mixture of operators and values");
                this.allDocs(function (doc) {
                    var _a;
                    log.debug("updating: %s", doc[_this.idPropertyName]);
                    summary.numAffected++;
                    summary.numUpdated++;
                    if (hasOps) {
                        for (var _i = 0, ops_1 = ops; _i < ops_1.length; _i++) {
                            var op = ops_1[_i];
                            if (op == '$set') {
                                for (var key in update[op]) {
                                    var value = update[op][key];
                                    log.debug("$set: %s -> %s", key, value);
                                    ObjectPath.set(doc, key, value);
                                }
                            }
                            else {
                                throw new Error("only $set is supported");
                            }
                        }
                    }
                    else {
                        doc = __assign((_a = {}, _a[_this.idPropertyName] = doc[_this.idPropertyName], _a), clone(update));
                    }
                    _this.writeDoc(doc);
                }, query, options);
                return [2, summary];
            });
        });
    };
    Collection.prototype.preprocessQuery = function (query) {
        query = __assign({}, query);
        if (query[this.idPropertyName] != null && typeof (query[this.idPropertyName] != 'string'))
            query[this.idPropertyName] = query[this.idPropertyName].toString();
        return query;
    };
    Collection.prototype.readDoc = function (id) {
        return __awaiter(this, void 0, void 0, function () {
            var log, doc_1, path, doc, _a, _b;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        log = this.log;
                        log.debug("readDoc: %s", id);
                        if (this.isCacheEnabled) {
                            doc_1 = this.cache[id];
                            if (doc_1 != null) {
                                log.debug("readDoc: %s -> cache hit", id);
                                return [2, clone(doc_1)];
                            }
                        }
                        path = this.makeFullPath(id);
                        log.debug("readDoc: loading %s from %s", id, path);
                        _b = (_a = JSON).parse;
                        return [4, readFile(path)];
                    case 1:
                        doc = _b.apply(_a, [(_c.sent()).toString()]);
                        if (this.isCacheEnabled) {
                            log.debug("readDoc: caching %s", id);
                            this.cache[id] = clone(doc);
                        }
                        return [2, doc];
                }
            });
        });
    };
    Collection.prototype.writeDoc = function (data) {
        return __awaiter(this, void 0, void 0, function () {
            var log, id, path, json, versionPath;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        log = this.log;
                        id = data[this.idPropertyName];
                        path = this.makeFullPath(id);
                        log.debug("writing: %s -> %s", id, path);
                        json = this.options.indent ? JSON.stringify(data, null, "\t") : JSON.stringify(data);
                        if (this.isCacheEnabled) {
                            log.debug("caching: %s", id);
                            this.cache[id] = JSON.parse(json);
                        }
                        if (!(this.options.history === true)) return [3, 2];
                        versionPath = this.makeFullPath(id, Date.now().toString());
                        return [4, writeFile(versionPath, json)];
                    case 1:
                        _a.sent();
                        _a.label = 2;
                    case 2: return [4, writeFile(path, json)];
                    case 3:
                        _a.sent();
                        log.debug("wrote: %s -> %s", id, path);
                        return [2];
                }
            });
        });
    };
    Collection.prototype.getAllDocFilenames = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4, readDir(this.path)];
                    case 1: return [2, (_a.sent()).filter(function (fn) { return /\.json$/i.test(fn); })];
                }
            });
        });
    };
    Collection.prototype.getIdFromFilename = function (path) {
        var fn = Path.basename(path);
        var ext = Path.extname(path);
        return fn.substr(0, fn.length - ext.length);
    };
    Collection.prototype.allDocs = function (matchFun, cond, options) {
        if (options === void 0) { options = {}; }
        return __awaiter(this, void 0, void 0, function () {
            var log, numMatched, docs, id, _i, docs_1, doc, id, files, numMatched, _a, files_2, file, id, doc, e_2;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        log = this.log;
                        return [4, this.initialize()];
                    case 1:
                        _b.sent();
                        if (!this.isCacheEnabled) return [3, 6];
                        numMatched = 0;
                        docs = void 0;
                        if (cond[this.idPropertyName] != null)
                            docs = [this.cache[cond[this.idPropertyName]]].filter(function (d) { return d != null; });
                        else {
                            docs = [];
                            for (id in this.cache)
                                docs.push(this.cache[id]);
                        }
                        log.debug("find *");
                        _i = 0, docs_1 = docs;
                        _b.label = 2;
                    case 2:
                        if (!(_i < docs_1.length)) return [3, 5];
                        doc = docs_1[_i];
                        if (options.limit != null && numMatched >= options.limit) {
                            log.debug("find limited reached");
                            return [3, 5];
                        }
                        id = doc[this.idPropertyName];
                        log.debug("find in cached: %s", id);
                        if (!this.testDoc(doc, cond, options)) return [3, 4];
                        numMatched++;
                        log.debug("find in cached: %s -> matched", id);
                        return [4, matchFun(doc)];
                    case 3:
                        _b.sent();
                        _b.label = 4;
                    case 4:
                        _i++;
                        return [3, 2];
                    case 5: return [3, 17];
                    case 6:
                        files = void 0;
                        if (!(cond[this.idPropertyName] != null)) return [3, 7];
                        files = [this.makeFullPath(cond[this.idPropertyName])];
                        return [3, 9];
                    case 7: return [4, this.getAllDocFilenames()];
                    case 8:
                        files = _b.sent();
                        _b.label = 9;
                    case 9:
                        numMatched = 0;
                        log.debug("find *");
                        _a = 0, files_2 = files;
                        _b.label = 10;
                    case 10:
                        if (!(_a < files_2.length)) return [3, 17];
                        file = files_2[_a];
                        if (options.limit != null && numMatched >= options.limit) {
                            log.debug("find limited reached");
                            return [3, 17];
                        }
                        id = this.getIdFromFilename(file);
                        log.info("find in file: %s (%s)", file, id);
                        _b.label = 11;
                    case 11:
                        _b.trys.push([11, 15, , 16]);
                        return [4, this.readDoc(id)];
                    case 12:
                        doc = _b.sent();
                        if (!this.testDoc(doc, cond, options)) return [3, 14];
                        numMatched++;
                        log.debug("find in file: %s -> matched", id);
                        return [4, matchFun(doc)];
                    case 13:
                        _b.sent();
                        _b.label = 14;
                    case 14: return [3, 16];
                    case 15:
                        e_2 = _b.sent();
                        log.error("Error: %s (%s), at %s", e_2.message, e_2.code, e_2.stack);
                        return [3, 16];
                    case 16:
                        _a++;
                        return [3, 10];
                    case 17: return [2];
                }
            });
        });
    };
    Collection.prototype.testDoc = function (doc, cond, options) {
        if (options === void 0) { options = {}; }
        var log = this.log;
        log.debug("testing %s against %s", JSON.stringify(doc), JSON.stringify(cond));
        for (var key in cond) {
            var value = ObjectPath.get(doc, key);
            log.debug("%s: %s ?= %s", key, value, cond[key]);
            if (value !== cond[key])
                return false;
        }
        log.debug("test -> matched");
        return true;
    };
    return Collection;
}());
exports.Collection = Collection;
//# sourceMappingURL=index.js.map