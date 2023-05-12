import { separator } from '../utils/constants.js';
import { termReader, termWriter } from './terms.js';
import { padNumStart } from './utils.js';
export const twoStepsQuadWriter = {
    subject: { type: '', value: '', lengths: '' },
    predicate: { type: '', value: '', lengths: '' },
    object: { type: '', value: '', lengths: '' },
    graph: { type: '', value: '', lengths: '' },
    ingest(quad, prefixes) {
        termWriter.write(quad.subject, this.subject, prefixes);
        termWriter.write(quad.predicate, this.predicate, prefixes);
        termWriter.write(quad.object, this.object, prefixes);
        termWriter.write(quad.graph, this.graph, prefixes);
        return this;
    },
    write(prefix, termNames) {
        let key = prefix;
        let lengths = '';
        for (let t = 0, term; t < termNames.length; t += 1) {
            term = this[termNames[t]];
            key += term.value + separator;
            lengths += term.type + term.lengths;
        }
        return key + lengths + padNumStart(lengths.length);
    },
};
export const quadReader = {
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
            this[termName] = termReader.read(key, this, factory, prefixes);
            this.keyOffset += separator.length;
        }
        return factory.quad(this.subject, this.predicate, this.object, this.graph);
    },
};
//# sourceMappingURL=quads.js.map