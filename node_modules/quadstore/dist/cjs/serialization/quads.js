"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.quadReader = exports.twoStepsQuadWriter = void 0;
const constants_1 = require("../utils/constants");
const terms_1 = require("./terms");
const utils_1 = require("./utils");
exports.twoStepsQuadWriter = {
    subject: { type: '', value: '', lengths: '' },
    predicate: { type: '', value: '', lengths: '' },
    object: { type: '', value: '', lengths: '' },
    graph: { type: '', value: '', lengths: '' },
    ingest(quad, prefixes) {
        terms_1.termWriter.write(quad.subject, this.subject, prefixes);
        terms_1.termWriter.write(quad.predicate, this.predicate, prefixes);
        terms_1.termWriter.write(quad.object, this.object, prefixes);
        terms_1.termWriter.write(quad.graph, this.graph, prefixes);
        return this;
    },
    write(prefix, termNames) {
        let key = prefix;
        let lengths = '';
        for (let t = 0, term; t < termNames.length; t += 1) {
            term = this[termNames[t]];
            key += term.value + constants_1.separator;
            lengths += term.type + term.lengths;
        }
        return key + lengths + (0, utils_1.padNumStart)(lengths.length);
    },
};
exports.quadReader = {
    subject: null,
    predicate: null,
    object: null,
    graph: null,
    keyOffset: 0,
    lengthsOffset: 0,
    read(key, keyOffset, termNames, factory, prefixes) {
        this.lengthsOffset = key.length - parseInt(key.slice(-4)) - 4;
        this.keyOffset = keyOffset;
        for (let t = 0, termName; t < termNames.length; t += 1) {
            termName = termNames[t];
            this[termName] = terms_1.termReader.read(key, this, factory, prefixes);
            this.keyOffset += constants_1.separator.length;
        }
        return factory.quad(this.subject, this.predicate, this.object, this.graph);
    },
};
//# sourceMappingURL=quads.js.map