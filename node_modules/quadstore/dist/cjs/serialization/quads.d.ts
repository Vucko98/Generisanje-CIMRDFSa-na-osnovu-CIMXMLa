import type { DataFactory, Term } from 'rdf-js';
import type { Prefixes, Quad, SerializedTerm, TermName } from '../types';
type TwoStepsQuadWriter = Record<TermName, SerializedTerm> & {
    ingest(quad: Quad, prefixes: Prefixes): TwoStepsQuadWriter;
    write(prefix: string, termNames: TermName[]): string;
};
export declare const twoStepsQuadWriter: TwoStepsQuadWriter;
type QuadReader = Record<TermName, Term | null> & {
    keyOffset: number;
    lengthsOffset: number;
} & {
    read(key: string, keyOffset: number, termNames: TermName[], factory: DataFactory, prefixes: Prefixes): Quad;
};
export declare const quadReader: QuadReader;
export {};
