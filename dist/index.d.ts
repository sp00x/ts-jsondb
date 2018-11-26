import { ILogger } from '@sp00x/log-interface';
export declare class Database {
    readonly log: ILogger;
    readonly path: string;
    private collections;
    private isInitialized;
    constructor(path: string, log?: ILogger);
    initialize(): Promise<void>;
    collection(name: string, options?: ICollectionOptions): Promise<Collection>;
}
export interface IFindOptions {
    limit?: number;
}
export declare class ObjectID {
    value: string;
    constructor(value?: string);
    toString(): string;
}
export interface OperationSummary {
    numAffected: number;
    numDeleted: number;
    numUpdated: number;
    numInserted: number;
}
export interface OperationSummaryEx extends OperationSummary {
    docs: Array<any>;
}
export interface ICollectionOptions {
    idPropertyName?: string;
    indent?: boolean;
    cache?: boolean;
    locking?: boolean;
    history?: boolean;
}
export declare class Collection {
    private log;
    private db;
    private name;
    private path;
    private isInitialized;
    private options;
    private cache;
    private idPropertyName;
    readonly isCacheEnabled: boolean;
    constructor(db: Database, name: string, options?: ICollectionOptions);
    initialize(): Promise<void>;
    makeFullPath(id: string | ObjectID, version?: string): string;
    private ensureId;
    save(doc: any): Promise<OperationSummaryEx>;
    find(query: any, options?: IFindOptions): Promise<Array<any>>;
    findOne(query: any, options?: IFindOptions): Promise<any>;
    delete(query: any, options?: IFindOptions): Promise<OperationSummary>;
    updateOne(query: any, update: any, options?: IFindOptions): Promise<OperationSummaryEx>;
    update(query: any, update: any, options?: IFindOptions): Promise<OperationSummaryEx>;
    private preprocessQuery;
    private readDoc;
    private writeDoc;
    private getAllDocFilenames;
    private getIdFromFilename;
    private allDocs;
    private testDoc;
}
