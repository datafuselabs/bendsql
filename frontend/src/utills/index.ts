export * from "./graph";

/**
 * Formats a given percentage value.
 * @param {number} numerator - The numerator of the percentage.
 * @param {number} denominator - The denominator of the percentage.
 * @returns {string} - The formatted percentage string.
 */
export function getPercent(numerator: number, denominator: number): string {
  if (denominator === 0) {
    return "0%";
  }
  const percent = (numerator / denominator) * 100;
  return `${percent.toFixed(1)}%`;
}


/**
 * Transforms the errors array by extracting the error type and merging it with the error details.
 * @param {any[]} errors - The array of errors to be transformed.
 * @returns {any[]} - The transformed array of errors.
 */
export function transformErrors(errors) {
  return errors.map(error => {
    const type = Object.keys(error)[0];
    return { _errorType: type, ...error[type] };
  });
}