export const sliceString = (source, offset, length) => {
    return source.slice(offset, offset + length);
};
export const padNumStart = (val) => {
    if (val < 10)
        return '000' + val;
    if (val < 100)
        return '00' + val;
    if (val < 1000)
        return '0' + val;
    if (val < 10000)
        return '' + val;
    throw new Error('too long: ' + val);
};
//# sourceMappingURL=utils.js.map