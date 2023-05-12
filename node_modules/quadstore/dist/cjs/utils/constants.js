"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.emptyValue = exports.levelDelOpts = exports.levelPutOpts = exports.defaultIndexes = exports.termNames = exports.separator = exports.boundary = exports.emptyObject = void 0;
exports.emptyObject = {};
exports.boundary = '\uDBFF\uDFFF';
exports.separator = '\u0000\u0000';
exports.termNames = [
    'subject',
    'predicate',
    'object',
    'graph',
];
exports.defaultIndexes = [
    ['subject', 'predicate', 'object', 'graph'],
    ['object', 'graph', 'subject', 'predicate'],
    ['graph', 'subject', 'predicate', 'object'],
    ['subject', 'object', 'predicate', 'graph'],
    ['predicate', 'object', 'graph', 'subject'],
    ['graph', 'predicate', 'object', 'subject'],
];
exports.levelPutOpts = {
    keyEncoding: 'utf8',
    valueEncoding: 'view',
};
exports.levelDelOpts = {
    keyEncoding: 'utf8',
};
exports.emptyValue = new Uint8Array(0);
//# sourceMappingURL=constants.js.map