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
exports.writePattern = void 0;
const xsd = __importStar(require("./xsd"));
const fpstring_1 = require("./fpstring");
const constants_1 = require("../utils/constants");
const terms_1 = require("./terms");
const serialized = {
    type: '',
    value: '',
    lengths: '',
};
const patternLiteralWriter = {
    write(term, prefixes) {
        if (term.language) {
            terms_1.langStringLiteralWriter.write(term, serialized, prefixes);
            return;
        }
        if (term.datatype) {
            switch (term.datatype.value) {
                case xsd.string:
                    terms_1.stringLiteralWriter.write(term, serialized, prefixes);
                    return;
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
                    terms_1.numericLiteralWriter.write(term, serialized, prefixes, true, (0, fpstring_1.encode)(term.value));
                    return;
                case xsd.dateTime:
                    terms_1.numericLiteralWriter.write(term, serialized, prefixes, true, (0, fpstring_1.encode)(new Date(term.value).valueOf()));
                    return;
                default:
                    terms_1.genericLiteralWriter.write(term, serialized, prefixes);
                    return;
            }
        }
        terms_1.stringLiteralWriter.write(term, serialized, prefixes);
        return;
    }
};
const writePattern = (pattern, index, prefixes) => {
    let gt = index.prefix;
    let lt = index.prefix;
    let gte = true;
    let lte = true;
    let didRange = false;
    let didLiteral = false;
    let remaining = Object.entries(pattern).filter(([termName, term]) => term).length;
    if (remaining === 0) {
        lt += constants_1.boundary;
        return { gt, lt, gte, lte, order: index.terms, index };
    }
    let t = 0;
    for (; t < index.terms.length && remaining > 0; t += 1) {
        const term = pattern[index.terms[t]];
        if (!term) {
            return null;
        }
        if (didRange || didLiteral) {
            return null;
        }
        switch (term.termType) {
            case 'Range':
                didRange = true;
                if (term.gt) {
                    patternLiteralWriter.write(term.gt, prefixes);
                    gt += serialized.value;
                    gte = false;
                }
                else if (term.gte) {
                    patternLiteralWriter.write(term.gte, prefixes);
                    gt += serialized.value;
                    gte = true;
                }
                if (term.lt) {
                    patternLiteralWriter.write(term.lt, prefixes);
                    lt += serialized.value;
                    lte = false;
                }
                else if (term.lte) {
                    patternLiteralWriter.write(term.lte, prefixes);
                    lt += serialized.value;
                    lte = true;
                }
                break;
            case 'Literal':
                didLiteral = true;
                patternLiteralWriter.write(term, prefixes);
                gt += serialized.value;
                gte = true;
                patternLiteralWriter.write(term, prefixes);
                lt += serialized.value;
                lte = true;
                break;
            case 'NamedNode':
                terms_1.namedNodeWriter.write(term, serialized, prefixes);
                gt += serialized.value;
                gte = true;
                terms_1.namedNodeWriter.write(term, serialized, prefixes);
                lt += serialized.value;
                lte = true;
                break;
            case 'BlankNode':
                terms_1.blankNodeWriter.write(term, serialized, prefixes);
                gt += serialized.value;
                gte = true;
                terms_1.blankNodeWriter.write(term, serialized, prefixes);
                lt += serialized.value;
                lte = true;
                break;
            case 'DefaultGraph':
                terms_1.defaultGraphWriter.write(term, serialized, prefixes);
                gt += serialized.value;
                gte = true;
                terms_1.defaultGraphWriter.write(term, serialized, prefixes);
                lt += serialized.value;
                lte = true;
                break;
        }
        remaining -= 1;
        if (remaining > 0 && t < index.terms.length - 1) {
            gt += constants_1.separator;
            lt += constants_1.separator;
        }
    }
    if (lte) {
        if (didRange || didLiteral) {
            lt += constants_1.boundary;
        }
        else {
            lt += constants_1.separator + constants_1.boundary;
        }
    }
    else {
        lt += constants_1.separator;
    }
    if (gte) {
        if (!didRange && !didLiteral) {
            gt += constants_1.separator;
        }
    }
    else {
        if (didRange || didLiteral) {
            gt += constants_1.boundary;
        }
        else {
            gt += constants_1.separator + constants_1.boundary;
        }
    }
    return { gt, lt, gte, lte, order: index.terms.slice(didRange ? t - 1 : 1), index };
};
exports.writePattern = writePattern;
//# sourceMappingURL=patterns.js.map