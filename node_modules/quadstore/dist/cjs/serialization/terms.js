"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.termReader = exports.termWriter = exports.defaultGraphReader = exports.defaultGraphWriter = exports.numericLiteralReader = exports.numericLiteralWriter = exports.langStringLiteralReader = exports.langStringLiteralWriter = exports.stringLiteralReader = exports.stringLiteralWriter = exports.genericLiteralReader = exports.genericLiteralWriter = exports.blankNodeReader = exports.blankNodeWriter = exports.namedNodeReader = exports.namedNodeWriter = void 0;
const xsd = __importStar(require("./xsd"));
const utils_1 = require("./utils");
const constants_1 = require("../utils/constants");
const fpstring_1 = require("./fpstring");
exports.namedNodeWriter = {
    write(node, serialized, prefixes) {
        const compactedIri = prefixes.compactIri(node.value);
        serialized.lengths = (0, utils_1.padNumStart)(compactedIri.length);
        serialized.value = compactedIri;
    },
};
exports.namedNodeReader = {
    read(key, state, factory, prefixes) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt((0, utils_1.sliceString)(key, lengthsOffset, 4));
        state.lengthsOffset += 4;
        state.keyOffset += valueLen;
        return factory.namedNode(prefixes.expandTerm((0, utils_1.sliceString)(key, keyOffset, valueLen)));
    },
};
exports.blankNodeWriter = {
    write(node, serialized) {
        serialized.lengths = (0, utils_1.padNumStart)(node.value.length);
        serialized.value = node.value;
    },
};
exports.blankNodeReader = {
    read(key, state, factory) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt((0, utils_1.sliceString)(key, lengthsOffset, 4));
        state.lengthsOffset += 4;
        state.keyOffset += valueLen;
        return factory.blankNode((0, utils_1.sliceString)(key, keyOffset, valueLen));
    },
};
exports.genericLiteralWriter = {
    write(node, serialized) {
        serialized.lengths = (0, utils_1.padNumStart)(node.value.length) + (0, utils_1.padNumStart)(node.datatype.value.length);
        serialized.value = node.datatype.value + constants_1.separator + node.value;
    },
};
exports.genericLiteralReader = {
    read(key, state, factory, prefixes) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt((0, utils_1.sliceString)(key, lengthsOffset, 4));
        const datatypeValueLen = parseInt((0, utils_1.sliceString)(key, lengthsOffset + 4, 4));
        state.lengthsOffset += 8;
        state.keyOffset += valueLen + datatypeValueLen + constants_1.separator.length;
        return factory.literal((0, utils_1.sliceString)(key, keyOffset + datatypeValueLen + constants_1.separator.length, valueLen), factory.namedNode((0, utils_1.sliceString)(key, keyOffset, datatypeValueLen)));
    },
};
exports.stringLiteralWriter = {
    write(node, serialized) {
        serialized.lengths = (0, utils_1.padNumStart)(node.value.length);
        serialized.value = node.value;
    },
};
exports.stringLiteralReader = {
    read(key, state, factory) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt((0, utils_1.sliceString)(key, lengthsOffset, 4));
        state.lengthsOffset += 4;
        state.keyOffset += valueLen;
        return factory.literal((0, utils_1.sliceString)(key, keyOffset, valueLen));
    },
};
exports.langStringLiteralWriter = {
    write(node, serialized) {
        serialized.lengths = (0, utils_1.padNumStart)(node.value.length) + (0, utils_1.padNumStart)(node.language.length);
        serialized.value = node.language + constants_1.separator + node.value;
    },
};
exports.langStringLiteralReader = {
    read(key, state, factory, prefixes) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt((0, utils_1.sliceString)(key, lengthsOffset, 4));
        const langCodeLen = parseInt((0, utils_1.sliceString)(key, lengthsOffset + 4, 4));
        state.lengthsOffset += 8;
        state.keyOffset += valueLen + langCodeLen + constants_1.separator.length;
        return factory.literal((0, utils_1.sliceString)(key, keyOffset + langCodeLen + constants_1.separator.length, valueLen), (0, utils_1.sliceString)(key, keyOffset, langCodeLen));
    },
};
exports.numericLiteralWriter = {
    write(node, serialized, prefixes, rangeMode, encodedValue) {
        serialized.lengths = (0, utils_1.padNumStart)(node.value.length) + (0, utils_1.padNumStart)(node.datatype.value.length) + (0, utils_1.padNumStart)(encodedValue.length);
        if (!rangeMode) {
            serialized.value = encodedValue + constants_1.separator + node.datatype.value + constants_1.separator + node.value;
        }
        else {
            serialized.value = encodedValue;
        }
    },
};
exports.numericLiteralReader = {
    read(key, state, factory, prefixes) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt((0, utils_1.sliceString)(key, lengthsOffset, 4));
        const datatypeValueLen = parseInt((0, utils_1.sliceString)(key, lengthsOffset + 4, 4));
        const numericValueLen = parseInt((0, utils_1.sliceString)(key, lengthsOffset + 8, 4));
        state.lengthsOffset += 12;
        state.keyOffset += numericValueLen + datatypeValueLen + valueLen + (constants_1.separator.length * 2);
        return factory.literal((0, utils_1.sliceString)(key, keyOffset + numericValueLen + constants_1.separator.length + datatypeValueLen + constants_1.separator.length, valueLen), factory.namedNode((0, utils_1.sliceString)(key, keyOffset + numericValueLen + constants_1.separator.length, datatypeValueLen)));
    },
};
exports.defaultGraphWriter = {
    write(node, serialized) {
        serialized.value = 'dg';
        serialized.lengths = '2';
    },
};
exports.defaultGraphReader = {
    read(key, state, factory, prefixes) {
        state.keyOffset += 2;
        state.lengthsOffset += 1;
        return factory.defaultGraph();
    },
};
exports.termWriter = {
    write(term, serialized, prefixes) {
        switch (term.termType) {
            case 'NamedNode':
                serialized.type = '0';
                exports.namedNodeWriter.write(term, serialized, prefixes);
                break;
            case 'BlankNode':
                serialized.type = '1';
                exports.blankNodeWriter.write(term, serialized, prefixes);
                break;
            case 'DefaultGraph':
                serialized.type = '6';
                exports.defaultGraphWriter.write(term, serialized, prefixes);
                break;
            case 'Literal':
                if (term.language) {
                    serialized.type = '4';
                    exports.langStringLiteralWriter.write(term, serialized, prefixes);
                }
                else if (term.datatype) {
                    switch (term.datatype.value) {
                        case xsd.string:
                            serialized.type = '3';
                            exports.stringLiteralWriter.write(term, serialized, prefixes);
                            break;
                        case xsd.integer:
                        case xsd.double:
                        case xsd.decimal:
                        case xsd.nonPositiveInteger:
                        case xsd.negativeInteger:
                        case xsd.long:
                        case xsd.int:
                        case xsd.short:
                        case xsd.byte:
                        case xsd.nonNegativeInteger:
                        case xsd.unsignedLong:
                        case xsd.unsignedInt:
                        case xsd.unsignedShort:
                        case xsd.unsignedByte:
                        case xsd.positiveInteger:
                            serialized.type = '5';
                            exports.numericLiteralWriter.write(term, serialized, prefixes, false, (0, fpstring_1.encode)(term.value));
                            break;
                        case xsd.dateTime:
                            serialized.type = '7';
                            exports.numericLiteralWriter.write(term, serialized, prefixes, false, (0, fpstring_1.encode)(new Date(term.value).valueOf()));
                            break;
                        default:
                            serialized.type = '2';
                            exports.genericLiteralWriter.write(term, serialized, prefixes);
                    }
                }
                else {
                    serialized.type = '3';
                    exports.stringLiteralWriter.write(term, serialized, prefixes);
                }
        }
    }
};
exports.termReader = {
    read(key, state, factory, prefixes) {
        let termValue;
        const encodedTermType = key.charAt(state.lengthsOffset);
        state.lengthsOffset += 1;
        switch (encodedTermType) {
            case '0':
                termValue = exports.namedNodeReader.read(key, state, factory, prefixes);
                break;
            case '1':
                termValue = exports.blankNodeReader.read(key, state, factory, prefixes);
                break;
            case '2':
                termValue = exports.genericLiteralReader.read(key, state, factory, prefixes);
                break;
            case '3':
                termValue = exports.stringLiteralReader.read(key, state, factory, prefixes);
                break;
            case '4':
                termValue = exports.langStringLiteralReader.read(key, state, factory, prefixes);
                break;
            case '5':
                termValue = exports.numericLiteralReader.read(key, state, factory, prefixes);
                break;
            case '6':
                termValue = exports.defaultGraphReader.read(key, state, factory, prefixes);
                break;
            case '7':
                termValue = exports.numericLiteralReader.read(key, state, factory, prefixes);
                break;
            default: throw new Error(`Unexpected encoded term type "${encodedTermType}"`);
        }
        return termValue;
    }
};
//# sourceMappingURL=terms.js.map