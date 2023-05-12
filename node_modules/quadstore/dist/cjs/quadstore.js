'use strict';
Object.defineProperty(exports, "__esModule", { value: true });
exports.Quadstore = void 0;
const types_1 = require("./types");
const events_1 = require("events");
const asynciterator_1 = require("asynciterator");
const stuff_1 = require("./utils/stuff");
const constants_1 = require("./utils/constants");
const consumeonebyone_1 = require("./utils/consumeonebyone");
const consumeinbatches_1 = require("./utils/consumeinbatches");
const uid_1 = require("./utils/uid");
const get_1 = require("./get");
const scope_1 = require("./scope");
const quads_1 = require("./serialization/quads");
class Quadstore {
    db;
    indexes;
    id;
    prefixes;
    dataFactory;
    constructor(opts) {
        (0, stuff_1.ensureAbstractLevel)(opts.backend, '"opts.backend"');
        this.dataFactory = opts.dataFactory;
        this.db = opts.backend;
        this.indexes = [];
        this.id = (0, uid_1.uid)();
        (opts.indexes || constants_1.defaultIndexes)
            .forEach((index) => this._addIndex(index));
        this.prefixes = opts.prefixes || {
            expandTerm: term => term,
            compactIri: iri => iri,
        };
    }
    ensureReady() {
        if (this.db.status !== 'open') {
            throw new Error(`Store is not ready (status: "${this.db.status}"). Did you call store.open()?`);
        }
    }
    async open() {
        if (this.db.status !== 'open') {
            await this.db.open();
        }
    }
    async close() {
        if (this.db.status !== 'closed') {
            await this.db.close();
        }
    }
    toString() {
        return this.toJSON();
    }
    toJSON() {
        return `[object ${this.constructor.name}::${this.id}]`;
    }
    _addIndex(terms) {
        const name = terms.map(t => t.charAt(0).toUpperCase()).join('');
        this.indexes.push({
            terms,
            prefix: name + constants_1.separator,
        });
    }
    async clear() {
        if (typeof this.db.clear === 'function') {
            return new Promise((resolve, reject) => {
                this.db.clear((err) => {
                    err ? reject(err) : resolve();
                });
            });
        }
        await this.delStream((await this.getStream({})).iterator, { batchSize: 20 });
    }
    match(subject, predicate, object, graph, opts = constants_1.emptyObject) {
        if (subject && subject.termType === 'Literal') {
            return new asynciterator_1.EmptyIterator();
        }
        const pattern = { subject, predicate, object, graph };
        return (0, asynciterator_1.wrap)(this.getStream(pattern, opts).then(results => results.iterator));
    }
    async countQuads(subject, predicate, object, graph, opts = constants_1.emptyObject) {
        if (subject && subject.termType === 'Literal') {
            return 0;
        }
        const pattern = { subject, predicate, object, graph };
        const results = await this.getApproximateSize(pattern, opts);
        return results.approximateSize;
    }
    import(source) {
        const emitter = new events_1.EventEmitter();
        this.putStream(source, {})
            .then(() => { emitter.emit('end'); })
            .catch((err) => { emitter.emit('error', err); });
        return emitter;
    }
    remove(source) {
        const emitter = new events_1.EventEmitter();
        this.delStream(source, {})
            .then(() => emitter.emit('end'))
            .catch((err) => emitter.emit('error', err));
        return emitter;
    }
    removeMatches(subject, predicate, object, graph, opts = constants_1.emptyObject) {
        const source = this.match(subject, predicate, object, graph, opts);
        return this.remove(source);
    }
    deleteGraph(graph) {
        return this.removeMatches(undefined, undefined, undefined, graph);
    }
    async getApproximateSize(pattern, opts = constants_1.emptyObject) {
        await this.ensureReady();
        return await (0, get_1.getApproximateSize)(this, pattern, opts);
    }
    _batchPut(quad, batch) {
        const { indexes, prefixes } = this;
        quads_1.twoStepsQuadWriter.ingest(quad, prefixes);
        for (let i = 0, il = indexes.length, index; i < il; i += 1) {
            index = indexes[i];
            const key = quads_1.twoStepsQuadWriter.write(index.prefix, index.terms);
            batch = batch.put(key, constants_1.emptyValue, constants_1.levelPutOpts);
        }
        return batch;
    }
    async put(quad, opts = constants_1.emptyObject) {
        this.ensureReady();
        const { indexes, db } = this;
        let batch = db.batch();
        if (opts.scope) {
            quad = opts.scope.parseQuad(quad, batch);
        }
        this._batchPut(quad, batch);
        await this.writeBatch(batch, opts);
        return { type: types_1.ResultType.VOID };
    }
    async multiPut(quads, opts = constants_1.emptyObject) {
        this.ensureReady();
        const { indexes, db } = this;
        let batch = db.batch();
        for (let q = 0, ql = quads.length, quad; q < ql; q += 1) {
            quad = quads[q];
            if (opts.scope) {
                quad = opts.scope.parseQuad(quad, batch);
            }
            this._batchPut(quad, batch);
        }
        await this.writeBatch(batch, opts);
        return { type: types_1.ResultType.VOID };
    }
    _batchDel(quad, batch) {
        const { indexes, prefixes } = this;
        quads_1.twoStepsQuadWriter.ingest(quad, prefixes);
        for (let i = 0, il = indexes.length, index; i < il; i += 1) {
            index = indexes[i];
            const key = quads_1.twoStepsQuadWriter.write(index.prefix, index.terms);
            batch = batch.del(key, constants_1.levelDelOpts);
        }
        return batch;
    }
    async del(quad, opts = constants_1.emptyObject) {
        this.ensureReady();
        const batch = this.db.batch();
        this._batchDel(quad, batch);
        await this.writeBatch(batch, opts);
        return { type: types_1.ResultType.VOID };
    }
    async multiDel(quads, opts = constants_1.emptyObject) {
        this.ensureReady();
        const batch = this.db.batch();
        for (let q = 0, ql = quads.length, quad; q < ql; q += 1) {
            quad = quads[q];
            this._batchDel(quad, batch);
        }
        await this.writeBatch(batch, opts);
        return { type: types_1.ResultType.VOID };
    }
    async patch(oldQuad, newQuad, opts = constants_1.emptyObject) {
        this.ensureReady();
        const { indexes, db } = this;
        const batch = db.batch();
        this._batchDel(oldQuad, batch);
        this._batchPut(newQuad, batch);
        await this.writeBatch(batch, opts);
        return { type: types_1.ResultType.VOID };
    }
    async multiPatch(oldQuads, newQuads, opts = constants_1.emptyObject) {
        this.ensureReady();
        const { indexes, db } = this;
        let batch = db.batch();
        for (let oq = 0, oql = oldQuads.length, oldQuad; oq < oql; oq += 1) {
            oldQuad = oldQuads[oq];
            this._batchDel(oldQuad, batch);
        }
        for (let nq = 0, nql = newQuads.length, newQuad; nq < nql; nq += 1) {
            newQuad = newQuads[nq];
            this._batchPut(newQuad, batch);
        }
        await this.writeBatch(batch, opts);
        return { type: types_1.ResultType.VOID };
    }
    async writeBatch(batch, opts) {
        if (opts.preWrite) {
            await opts.preWrite(batch);
        }
        await batch.write();
    }
    async get(pattern, opts = constants_1.emptyObject) {
        this.ensureReady();
        const results = await this.getStream(pattern, opts);
        const items = await (0, stuff_1.streamToArray)(results.iterator);
        return {
            items,
            type: results.type,
            order: results.order,
            index: results.index,
            resorted: results.resorted,
        };
    }
    async getStream(pattern, opts = constants_1.emptyObject) {
        this.ensureReady();
        return await (0, get_1.getStream)(this, pattern, opts);
    }
    async putStream(source, opts = constants_1.emptyObject) {
        this.ensureReady();
        const batchSize = opts.batchSize || 1;
        if (batchSize === 1) {
            await (0, consumeonebyone_1.consumeOneByOne)(source, quad => this.put(quad, opts));
        }
        else {
            await (0, consumeinbatches_1.consumeInBatches)(source, batchSize, quads => this.multiPut(quads, opts));
        }
        return { type: types_1.ResultType.VOID };
    }
    async delStream(source, opts = constants_1.emptyObject) {
        this.ensureReady();
        const batchSize = opts.batchSize || 1;
        if (batchSize === 1) {
            await (0, consumeonebyone_1.consumeOneByOne)(source, quad => this.del(quad));
        }
        else {
            await (0, consumeinbatches_1.consumeInBatches)(source, batchSize, quads => this.multiDel(quads));
        }
        return { type: types_1.ResultType.VOID };
    }
    async initScope() {
        await this.ensureReady();
        return await scope_1.Scope.init(this);
    }
    async loadScope(scopeId) {
        await this.ensureReady();
        return await scope_1.Scope.load(this, scopeId);
    }
    async deleteScope(scopeId) {
        await this.ensureReady();
        await scope_1.Scope.delete(this, scopeId);
    }
    async deleteAllScopes() {
        await this.ensureReady();
        await scope_1.Scope.delete(this);
    }
}
exports.Quadstore = Quadstore;
//# sourceMappingURL=quadstore.js.map