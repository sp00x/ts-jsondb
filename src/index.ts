import FS = require('fs');
import Path = require('path');
import Util = require('util');

import * as escapeStringRegexp from 'escape-string-regexp';
import { v4 as uuid } from 'uuid';
import * as ObjectPath from 'object-path';
import * as LockFile from 'lockfile';

import { ILogger, NullLogger, PrefixedLogger } from '@sp00x/log-interface';

const mkdirp = Util.promisify(require('mkdirp'));
const writeFile = Util.promisify(FS.writeFile);
const readFile = Util.promisify(FS.readFile);
const readDir = Util.promisify(FS.readdir);
const deleteFile = Util.promisify(FS.unlink);
const lockFile: (path: string, options: LockFile.Options) => Promise<void> = Util.promisify(LockFile.lock);
const unlockFile = Util.promisify(LockFile.unlock);

const DEFAULT_ID_PROPERTY_NAME: string = '_id';
const LOCK_FILE_EXT: string = '.lock';
const DATA_FILE_EXT: string = '.json';

const HISTORY_DATA_FILE_EXT: string = '.json.%{version}'; // make sure *${DATA_FILE_EXT} does not match this!
const HISTORY_DATA_SUB_PATH: string = 'history';

export class Database
{
    readonly log: ILogger;
    readonly path: string;
    private collections: {[name: string]: Collection} = {};
    private isInitialized: boolean = false;

    constructor(path: string, log?: ILogger)
    {
        this.log = log != null ? log : new NullLogger();
        this.path = path;
    }

    async initialize(): Promise<void>
    {
        if (!this.isInitialized)
        {
            await mkdirp(this.path);
            this.isInitialized = true;
        }
    }

    async collection(name: string, options: ICollectionOptions = {}): Promise<Collection>
    {
        await this.initialize();

        let c = this.collections[name];
        if (c != null) return c;
        c = new Collection(this, name, options);
        this.collections[name] = c;

        return c;
    }
}

export interface IFindOptions
{
    limit?: number;
}

export class ObjectID
{
    value: string;

    constructor(value?: string)
    {
        this.value = (value != null) ? value : uuid();
    }

    toString()
    {
        return this.value;
    }
}

export interface OperationSummary
{
    numAffected: number;
    numDeleted: number;
    numUpdated: number;
    numInserted: number;
}

export interface OperationSummaryEx extends OperationSummary
{
    docs: Array<any>;
}

export interface ICollectionOptions
{
    idPropertyName?: string;
    indent?: boolean;
    cache?: boolean;
    locking?: boolean;
    history?: boolean;
}

function clone(obj: any): any
{
    return JSON.parse(JSON.stringify(obj));
}

export class Collection
{
    private log: ILogger;
    private db: Database;
    private name: string;
    private path: string;
    private isInitialized: boolean = false;
    private options: ICollectionOptions;
    private cache: {[id: string]: any} = {};
    private idPropertyName: string;

    get isCacheEnabled(): boolean
    {
        return this.options.cache === true;
    }

    constructor(db: Database, name: string, options: ICollectionOptions = {})
    {
        this.db = db;
        this.name = name;
        this.log = db.log instanceof NullLogger ? db.log : new PrefixedLogger("<"+name+"> ", db.log);
        this.options = options;
        this.idPropertyName = (typeof this.options.idPropertyName == 'string') ? this.options.idPropertyName : DEFAULT_ID_PROPERTY_NAME;
        this.path = Path.join(this.db.path, this.name);
    }

    async initialize(): Promise<void>
    {
        if (!this.isInitialized)
        {
            await mkdirp(this.path);
            if (this.options.history === true) {
                await mkdirp(Path.join(this.path, HISTORY_DATA_SUB_PATH));
            }

            if (this.options.cache)
            {
                let files = await this.getAllDocFilenames();
                for (let fn of files)
                {
                    let id = this.getIdFromFilename(fn);
                    await this.readDoc(id);
                }
            }
            this.isInitialized = true;
        }
    }

    makeFullPath(id: string | ObjectID, version?: string): string
    {
        return (version == undefined)
            ? Path.join(this.path, id.toString()) + DATA_FILE_EXT
            : Path.join(this.path, HISTORY_DATA_SUB_PATH, id.toString()) + HISTORY_DATA_FILE_EXT.replace('%{version}', version);
    }

    private ensureId(doc: any): string
    {
        let id = (doc[this.idPropertyName] == null) ? uuid() : doc[this.idPropertyName].toString();
        doc[this.idPropertyName] = id;
        return id;
    }

    async save(doc: any): Promise<OperationSummaryEx>
    {
        const { log } = this;

        await this.initialize();

        if (doc == null || typeof doc != 'object') throw new Error("Not an object");
        let ins: any = {
            ...<any>doc
        }
        let id = this.ensureId(ins);

        log.debug("saving: %s", id);

        await this.writeDoc(ins);

        return {
            numAffected: 1,
            numInserted: 1,
            numDeleted: 0,
            numUpdated: 0,
            docs: [ ins ]
        }
    }
    
    async find(query: any, options: IFindOptions = {}): Promise<Array<any>>
    {
        const { log } = this;
        query = this.preprocessQuery(query);
        
        let docs: any[] = [];
        await this.allDocs(
            async (doc: any) => docs.push(doc),
            query,
            options
        );
        
        return docs;
    }

    async findOne(query: any, options: IFindOptions = {}): Promise<any>
    {
        return (await this.find(query, { limit: 1, ...options }))[0];
    }

    
    async delete(query: any, options: IFindOptions = {}): Promise<OperationSummary>
    {
        const { log } = this;

        await this.initialize();

        query = this.preprocessQuery(query);

        log.debug("delete where %j", query);

        let summary: OperationSummary = {
            numAffected: 0,
            numDeleted: 0,
            numUpdated: 0,
            numInserted: 0
        };

        let docs: any[] = [];
        await this.allDocs(async (doc: any) =>
        {
            log.debug("delete callback: %j", doc);
            let fn = this.makeFullPath(doc[this.idPropertyName]);
            try
            {
                log.debug("deleting: %s", fn);

                await deleteFile(fn);
                if (this.isCacheEnabled)
                {
                    let id = doc[this.idPropertyName];
                    delete this.cache[id];
                }
                summary.numAffected++;
                summary.numDeleted++;
            }
            catch (e)
            {
                log.error("Error deleting file: %s - %s (%s)", fn, e.message, e.code);
            }
        }, query, options);

        return summary;
    }

    async updateOne(query: any, update: any, options: IFindOptions = {}): Promise<OperationSummaryEx>
    {
        return await this.update(query, update, { ...options, limit: 1 });
    }

    async update(query: any, update: any, options: IFindOptions = {}): Promise<OperationSummaryEx>
    {
        const { log } = this;

        let summary: OperationSummaryEx = {
            docs: [],
            numAffected: 0,
            numDeleted: 0,
            numInserted: 0,
            numUpdated: 0
        };

        let keys = Object.keys(update);
        let ops = keys.filter(k => /^\$/.test(k));
        let hasOps = ops.length > 0;
        if (hasOps && ops.length != keys.length) throw new Error("Update document can not be a mixture of operators and values");

        this.allDocs((doc: any) =>
        {  
            log.debug("updating: %s", doc[this.idPropertyName]);

            summary.numAffected++;
            summary.numUpdated++;

            if (hasOps)
            {
                // perform ops
                for (let op of ops)
                {
                    if (op == '$set')
                    {
                        for (let key in update[op])
                        {
                            let value = update[op][key];
                            log.debug("$set: %s -> %s", key, value);
                            ObjectPath.set(doc, key, value);
                        }
                    }
                    else
                    {
                        throw new Error("only $set is supported")
                    }
                }
            }
            else
            {
                // whole doc update
                doc = {
                    [this.idPropertyName]: doc[this.idPropertyName], // just in case _id is omitted
                    ...clone(update)
                };
            }
            this.writeDoc(doc);
        }, query, options)

        return summary;
    }

    private preprocessQuery(query: any): any
    {
        query = { ...query };
        if (query[this.idPropertyName] != null && typeof(query[this.idPropertyName] != 'string')) 
            query[this.idPropertyName] = query[this.idPropertyName].toString(); 
        return query;
    }

    private async readDoc(id: string): Promise<any>
    {
        const { log } = this;
        log.debug("readDoc: %s", id);
        if (this.isCacheEnabled)
        {
            let doc = this.cache[id];
            if (doc != null)
            {
                log.debug("readDoc: %s -> cache hit", id);
                return clone(doc);
            }
        }
        let path = this.makeFullPath(id);
        log.debug("readDoc: loading %s from %s", id, path);
        let doc = JSON.parse((await readFile(path)).toString());
        if (this.isCacheEnabled)
        {
            log.debug("readDoc: caching %s", id);
            this.cache[id] = clone(doc);
        }
        return doc;
    }

    private async writeDoc(data: any): Promise<void>
    {
        const { log } = this;
        let id = data[this.idPropertyName];
        let path = this.makeFullPath(id);
        log.debug("writing: %s -> %s", id, path);
        let json = this.options.indent ? JSON.stringify(data, null, "\t") : JSON.stringify(data);
        if (this.isCacheEnabled)
        {
            log.debug("caching: %s", id);
            this.cache[id] = JSON.parse(json); // cache a copy
        }
        if (this.options.history === true) {
            let versionPath = this.makeFullPath(id, Date.now().toString());
            await writeFile(versionPath, json);
        }
        await writeFile(path, json);
        log.debug("wrote: %s -> %s", id, path);
    }

    private async getAllDocFilenames(): Promise<string[]>
    {
        let regex = new RegExp(escapeStringRegexp(DATA_FILE_EXT) + '$', "i");
        return (await readDir(this.path)).filter(fn => regex.test(fn));        
    }

    private getIdFromFilename(path: string): string
    {
        let fn = Path.basename(path);
        let ext = Path.extname(path);
        return fn.substr(0, fn.length - ext.length);
    }

    private async allDocs(matchFun: Function, cond: any, options: IFindOptions = {}): Promise<void>
    {
        const { log } = this;

        await this.initialize();

        if (this.isCacheEnabled)
        {
            let numMatched = 0;
            let docs: any[];
            if (cond[this.idPropertyName] != null)
                docs = [ this.cache[cond[this.idPropertyName]] ].filter(d => d != null);
            else
            {
                docs = [];
                for (let id in this.cache) docs.push(this.cache[id]);
            }

            log.debug("find *");
            for (let doc of docs)
            {
                if (options.limit != null && numMatched >= options.limit)
                {
                    log.debug("find limited reached");
                    break;
                }

                const id = doc[this.idPropertyName];   
                log.debug("find in cached: %s", id);
                if (this.testDoc(doc, cond, options))
                {
                    numMatched++;
                    log.debug("find in cached: %s -> matched", id);
                    await matchFun(doc);
                }
            }
        }
        else
        {
            let files: string[];

            if (cond[this.idPropertyName] != null)
                files = [ this.makeFullPath(cond[this.idPropertyName]) ];
            else
                files = await this.getAllDocFilenames();

            let numMatched = 0;
            log.debug("find *");
            for (let file of files)
            {
                if (options.limit != null && numMatched >= options.limit)
                {
                    log.debug("find limited reached");
                    break;
                }

                let id = this.getIdFromFilename(file);
                log.info("find in file: %s (%s)", file, id);
                try
                {
                    let doc = await this.readDoc(id);
                    if (this.testDoc(doc, cond, options))
                    {
                        numMatched++;
                        log.debug("find in file: %s -> matched", id);
                        await matchFun(doc);
                    }
                }
                catch (e)
                {
                    log.error("Error: %s (%s), at %s", e.message, e.code, e.stack);
                }
            }
        }
    }

    private testDoc(doc: any, cond: any, options: IFindOptions = {}): boolean
    {
        const { log } = this;

        log.debug("testing %s against %s", JSON.stringify(doc), JSON.stringify(cond));
        for (let key in cond)
        {
            let value = ObjectPath.get(doc, key);
            log.debug("%s: %s ?= %s", key, value, cond[key]);
            if (value !== cond[key]) return false;
        }
        log.debug("test -> matched");
        return true;
    }
}
