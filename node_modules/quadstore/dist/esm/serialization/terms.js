import * as xsd from './xsd.js';
import { padNumStart, sliceString } from './utils.js';
import { separator } from '../utils/constants.js';
import { encode } from './fpstring.js';
export const namedNodeWriter = {
    write(node, serialized, prefixes) {
        const compactedIri = prefixes.compactIri(node.value);
        serialized.lengths = padNumStart(compactedIri.length);
        serialized.value = compactedIri;
    },
};
export const namedNodeReader = {
    read(key, state, factory, prefixes) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt(sliceString(key, lengthsOffset, 4));
        state.lengthsOffset += 4;
        state.keyOffset += valueLen;
        return factory.namedNode(prefixes.expandTerm(sliceString(key, keyOffset, valueLen)));
    },
};
export const blankNodeWriter = {
    write(node, serialized) {
        serialized.lengths = padNumStart(node.value.length);
        serialized.value = node.value;
    },
};
export const blankNodeReader = {
    read(key, state, factory) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt(sliceString(key, lengthsOffset, 4));
        state.lengthsOffset += 4;
        state.keyOffset += valueLen;
        return factory.blankNode(sliceString(key, keyOffset, valueLen));
    },
};
export const genericLiteralWriter = {
    write(node, serialized) {
        serialized.lengths = padNumStart(node.value.length) + padNumStart(node.datatype.value.length);
        serialized.value = node.datatype.value + separator + node.value;
    },
};
export const genericLiteralReader = {
    read(key, state, factory, prefixes) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt(sliceString(key, lengthsOffset, 4));
        const datatypeValueLen = parseInt(sliceString(key, lengthsOffset + 4, 4));
        state.lengthsOffset += 8;
        state.keyOffset += valueLen + datatypeValueLen + separator.length;
        return factory.literal(sliceString(key, keyOffset + datatypeValueLen + separator.length, valueLen), factory.namedNode(sliceString(key, keyOffset, datatypeValueLen)));
    },
};
export const stringLiteralWriter = {
    write(node, serialized) {
        serialized.lengths = padNumStart(node.value.length);
        serialized.value = node.value;
    },
};
export const stringLiteralReader = {
    read(key, state, factory) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt(sliceString(key, lengthsOffset, 4));
        state.lengthsOffset += 4;
        state.keyOffset += valueLen;
        return factory.literal(sliceString(key, keyOffset, valueLen));
    },
};
export const langStringLiteralWriter = {
    write(node, serialized) {
        serialized.lengths = padNumStart(node.value.length) + padNumStart(node.language.length);
        serialized.value = node.language + separator + node.value;
    },
};
export const langStringLiteralReader = {
    read(key, state, factory, prefixes) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt(sliceString(key, lengthsOffset, 4));
        const langCodeLen = parseInt(sliceString(key, lengthsOffset + 4, 4));
        state.lengthsOffset += 8;
        state.keyOffset += valueLen + langCodeLen + separator.length;
        return factory.literal(sliceString(key, keyOffset + langCodeLen + separator.length, valueLen), sliceString(key, keyOffset, langCodeLen));
    },
};
export const numericLiteralWriter = {
    write(node, serialized, prefixes, rangeMode, encodedValue) {
        serialized.lengths = padNumStart(node.value.length) + padNumStart(node.datatype.value.length) + padNumStart(encodedValue.length);
        if (!rangeMode) {
            serialized.value = encodedValue + separator + node.datatype.value + separator + node.value;
        }
        else {
            serialized.value = encodedValue;
        }
    },
};
export const numericLiteralReader = {
    read(key, state, factory, prefixes) {
        const { keyOffset, lengthsOffset } = state;
        const valueLen = parseInt(sliceString(key, lengthsOffset, 4));
        const datatypeValueLen = parseInt(sliceString(key, lengthsOffset + 4, 4));
        const numericValueLen = parseInt(sliceString(key, lengthsOffset + 8, 4));
        state.lengthsOffset += 12;
        state.keyOffset += numericValueLen + datatypeValueLen + valueLen + (separator.length * 2);
        return factory.literal(sliceString(key, keyOffset + numericValueLen + separator.length + datatypeValueLen + separator.length, valueLen), factory.namedNode(sliceString(key, keyOffset + numericValueLen + separator.length, datatypeValueLen)));
    },
};
export const defaultGraphWriter = {
    write(node, serialized) {
        serialized.value = 'dg';
        serialized.lengths = '2';
    },
};
export const defaultGraphReader = {
    read(key, state, factory, prefixes) {
        state.keyOffset += 2;
        state.lengthsOffset += 1;
        return factory.defaultGraph();
    },
};
export const termWriter = {
    write(term, serialized, prefixes) {
        switch (term.termType) {
            case 'NamedNode':
                serialized.type = '0';
                namedNodeWriter.write(term, serialized, prefixes);
                break;
            case 'BlankNode':
                serialized.type = '1';
                blankNodeWriter.write(term, serialized, prefixes);
                break;
            case 'DefaultGraph':
                serialized.type = '6';
                defaultGraphWriter.write(term, serialized, prefixes);
                break;
            case 'Literal':
                if (term.language) {
                    serialized.type = '4';
                    langStringLiteralWriter.write(term, serialized, prefixes);
                }
                else if (term.datatype) {
                    switch (term.datatype.value) {
                        case xsd.string:
                            serialized.type = '3';
                            stringLiteralWriter.write(term, serialized, prefixes);
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
                            numericLiteralWriter.write(term, serialized, prefixes, false, encode(term.value));
                            break;
                        case xsd.dateTime:
                            serialized.type = '7';
                            numericLiteralWriter.write(term, serialized, prefixes, false, encode(new Date(term.value).valueOf()));
                            break;
                        default:
                            serialized.type = '2';
                            genericLiteralWriter.write(term, serialized, prefixes);
                    }
                }
                else {
                    serialized.type = '3';
                    stringLiteralWriter.write(term, serialized, prefixes);
                }
        }
    }
};
export const termReader = {
    read(key, state, factory, prefixes) {
        let termValue;
        const encodedTermType = key.charAt(state.lengthsOffset);
        state.lengthsOffset += 1;
        switch (encodedTermType) {
            case '0':
                termValue = namedNodeReader.read(key, state, factory, prefixes);
                break;
            case '1':
                termValue = blankNodeReader.read(key, state, factory, prefixes);
                break;
            case '2':
                termValue = genericLiteralReader.read(key, state, factory, prefixes);
                break;
            case '3':
                termValue = stringLiteralReader.read(key, state, factory, prefixes);
                break;
            case '4':
                termValue = langStringLiteralReader.read(key, state, factory, prefixes);
                break;
            case '5':
                termValue = numericLiteralReader.read(key, state, factory, prefixes);
                break;
            case '6':
                termValue = defaultGraphReader.read(key, state, factory, prefixes);
                break;
            case '7':
                termValue = numericLiteralReader.read(key, state, factory, prefixes);
                break;
            default: throw new Error(`Unexpected encoded term type "${encodedTermType}"`);
        }
        return termValue;
    }
};
//# sourceMappingURL=terms.js.map