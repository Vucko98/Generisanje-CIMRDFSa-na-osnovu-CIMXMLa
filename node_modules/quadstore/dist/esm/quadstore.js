'use strict';
import { ResultType } from './types/index.js';
import { EventEmitter } from 'events';
import { EmptyIterator, wrap, } from 'asynciterator';
import { streamToArray, ensureAbstractLevel, } from './utils/stuff.js';
import { emptyObject, defaultIndexes, separator, levelPutOpts, levelDelOpts, emptyValue, } from './utils/constants.js';
import { consumeOneByOne } from './utils/consumeonebyone.js';
import { consumeInBatches } from './utils/consumeinbatches.js';
import { uid } from './utils/uid.js';
import { getApproximateSize, getStream } from './get/index.js';
import { Scope } from './scope/index.js';
import { twoStepsQuadWriter } from './serialization/quads.js';
export class Quadstore {
    db;
    indexes;
    id;
    prefixes;
    dataFactory;
    constructor(opts) {
        ensureAbstractLevel(opts.backend, '"opts.backend"');
        this.dataFactory = opts.dataFactory;
        this.db = opts.backend;
        this.indexes = [];
        this.id = uid();
        (opts.indexes || defaultIndexes)
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
            prefix: name + separator,
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
    match(subject, predicate, object, graph, opts = emptyObject) {
        if (subject && subject.termType === 'Literal') {
            return new EmptyIterator();
        }
        const pattern = { subject, predicate, object, graph };
        return wrap(this.getStream(pattern, opts).then(results => results.iterator));
    }
    async countQuads(subject, predicate, object, graph, opts = emptyObject) {
        if (subject && subject.termType === 'Literal') {
            return 0;
        }
        const pattern = { subject, predicate, object, graph };
        const results = await this.getApproximateSize(pattern, opts);
        return results.approximateSize;
    }
    import(source) {
        const emitter = new EventEmitter();
        this.putStream(source, {})
            .then(() => { emitter.emit('end'); })
            .catch((err) => { emitter.emit('error', err); });
        return emitter;
    }
    remove(source) {
        const emitter = new EventEmitter();
        this.delStream(source, {})
            .then(() => emitter.emit('end'))
            .catch((err) => emitter.emit('error', err));
        return emitter;
    }
    removeMatches(subject, predicate, object, graph, opts = emptyObject) {
        const source = this.match(subject, predicate, object, graph, opts);
        return this.remove(source);
    }
    deleteGraph(graph) {
        return this.removeMatches(undefined, undefined, undefined, graph);
    }
    async getApproximateSize(pattern, opts = emptyObject) {
        await this.ensureReady();
        return await getApproximateSize(this, pattern, opts);
    }
    _batchPut(quad, batch) {
        const { indexes, prefixes } = this;
        twoStepsQuadWriter.ingest(quad, prefixes);
        for (let i = 0, il = indexes.length, index; i < il; i += 1) {
            index = indexes[i];
            const key = twoStepsQuadWriter.write(index.prefix, index.terms);
            batch = batch.put(key, emptyValue, levelPutOpts);
        }
        return batch;
    }
    async put(quad, opts = emptyObject) {
        this.ensureReady();
        const { indexes, db } = this;
        let batch = db.batch();
        if (opts.scope) {
            quad = opts.scope.parseQuad(quad, batch);
        }
        this._batchPut(quad, batch);
        await this.writeBatch(batch, opts);
        return { type: ResultType.VOID };
    }
    async multiPut(quads, opts = emptyObject) {
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
        return { type: ResultType.VOID };
    }
    _batchDel(quad, batch) {
        const { indexes, prefixes } = this;
        twoStepsQuadWriter.ingest(quad, prefixes);
        for (let i = 0, il = indexes.length, index; i < il; i += 1) {
            index = indexes[i];
            const key = twoStepsQuadWriter.write(index.prefix, index.terms);
            batch = batch.del(key, levelDelOpts);
        }
        return batch;
    }
    async del(quad, opts = emptyObject) {
        this.ensureReady();
        const batch = this.db.batch();
        this._batchDel(quad, batch);
        await this.writeBatch(batch, opts);
        return { type: ResultType.VOID };
    }
    async multiDel(quads, opts = emptyObject) {
        this.ensureReady();
        const batch = this.db.batch();
        for (let q = 0, ql = quads.length, quad; q < ql; q += 1) {
            quad = quads[q];
            this._batchDel(quad, batch);
        }
        await this.writeBatch(batch, opts);
        return { type: ResultType.VOID };
    }
    async patch(oldQuad, newQuad, opts = emptyObject) {
        this.ensureReady();
        const { indexes, db } = this;
        const batch = db.batch();
        this._batchDel(oldQuad, batch);
        this._batchPut(newQuad, batch);
        await this.writeBatch(batch, opts);
        return { type: ResultType.VOID };
    }
    async multiPatch(oldQuads, newQuads, opts = emptyObject) {
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
        return { type: ResultType.VOID };
    }
    async writeBatch(batch, opts) {
        if (opts.preWrite) {
            await opts.preWrite(batch);
        }
        await batch.write();
    }
    async get(pattern, opts = emptyObject) {
        this.ensureReady();
        const results = await this.getStream(pattern, opts);
        const items = await streamToArray(results.iterator);
        return {
            items,
            type: results.type,
            order: results.order,
            index: results.index,
            resorted: results.resorted,
        };
    }
    async getStream(pattern, opts = emptyObject) {
        this.ensureReady();
        return await getStream(this, pattern, opts);
    }
    async putStream(source, opts = emptyObject) {
        this.ensureReady();
        const batchSize = opts.batchSize || 1;
        if (batchSize === 1) {
            await consumeOneByOne(source, quad => this.put(quad, opts));
        }
        else {
            await consumeInBatches(source, batchSize, quads => this.multiPut(quads, opts));
        }
        return { type: ResultType.VOID };
    }
    async delStream(source, opts = emptyObject) {
        this.ensureReady();
        const batchSize = opts.batchSize || 1;
        if (batchSize === 1) {
            await consumeOneByOne(source, quad => this.del(quad));
        }
        else {
            await consumeInBatches(source, batchSize, quads => this.multiDel(quads));
        }
        return { type: ResultType.VOID };
    }
    async initScope() {
        await this.ensureReady();
        return await Scope.init(this);
    }
    async loadScope(scopeId) {
        await this.ensureReady();
        return await Scope.load(this, scopeId);
    }
    async deleteScope(scopeId) {
        await this.ensureReady();
        await Scope.delete(this, scopeId);
    }
    async deleteAllScopes() {
        await this.ensureReady();
        await Scope.delete(this);
    }
}
//# sourceMappingURL=quadstore.js.map