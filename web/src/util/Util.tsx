import React from "react";

// Given a link ensure that it has an attached protocol and add https if none is found
export const formatUrl = (link: any) =>
  link.startsWith("http") ? link : `https://${link}`;

// Given an array of JSX elements, return JSX that joins them with the Joiner.
export const joinArrayOfElements = (array: any, joiner: any) => {
  if (!array || !array.length) {
    return undefined;
  }
  return array.map((item: any, idx: any) => {
    if (!item) return undefined;
    return idx < array.length - 1 ? (
      <React.Fragment key={`join-${item.key}`}>
        {item}
        {joiner}
      </React.Fragment>
    ) : (
      <React.Fragment key={`join-${item.key}`}>{item}</React.Fragment>
    );
  });
};

// return unique elements of array
export const unique = (array: any, propertyName: any) =>
  array //
    .filter(
      (e: any, i: any) =>
        array //
          .findIndex((a: any) =>
            propertyName ? a[propertyName] === e[propertyName] : a === e
          ) === i
    );

export const flattenDeep = (arr: any) =>
  Array.isArray(arr)
    ? arr.reduce((a, b) => a.concat(flattenDeep(b)), [])
    : [arr];

export const isEmpty = (obj: any) =>
  (typeof obj === "object" && !Object.entries(obj || {}).length) || !obj;

// compare two objects
export const deepEqual = (x: any, y: any) =>
  x && y && typeof x === "object" && typeof x === typeof y
    ? Object.keys(x).length === Object.keys(y).length &&
      Object.keys(x).every((key) => deepEqual(x[key], y[key]))
    : x === y;

// return unique elements (not undefined) of array sorted by frequency of repetition
export const uniqueAndSortByFrequency = (array: any) => {
  const frequency = {};

  // compute frequencies of each value
  array.forEach((value: any) => {
    // remove undefined values
    if (value) {
      frequency[value] = frequency[value] ? frequency[value] + 1 : 1;
    }
  });

  // make array from the frequency object to de-duplicate
  const uniques = Object.keys(frequency);

  // sort the uniques array in descending order by frequency
  const compareFrequency = (a: any, b: any) => frequency[b] - frequency[a];

  return uniques.sort(compareFrequency);
};

// /**
//  * checkFeatureFlagActivated(featureFlagList, componentStateObject)
//  * Check if the feature flags have been set. If they have not, activate the function to check
//  * if the related cookies are set.
//  * @param {string[]} featureFlagList - The list of the feature flags we want to set.
//  * @param {object} componentStateObject - The object that points to the state object of
//  * the component. The feature flag will change the state of the component through it.
//  */
// export const checkFeatureFlagActivated = (
//   featureFlagList: any,
//   componentStateObject: any
// ) => {
//   featureFlagList.forEach((item: any) => {
//     if (!componentStateObject[item]) {
//       FeatureFlags.utils.activateFeature(item);
//     }
//   });
// };

/**
 * truncateStringOnWhitespace(str, maxLength)
 * Return a version of the string shortened to the provided maxLength param. This includes
 * the three characters for the ellipsis that is appended. If the string is shorter than the
 * max length it is returned as is. If the string contains no whitespace before the max
 * length it is truncated at that point regardless of word breaks.
 * @param {string} str - The string to be shortened (or returned without change)
 * @param {int} maxLength - The maximum length of the returned string to be applied
 */
export const truncateStringOnWhitespace = (str: any, maxLength: any) => {
  if (str.length < maxLength) {
    return str;
  }
  const truncStr = str.substr(0, maxLength - 3);
  const truncArray = truncStr.split(/\s+/).slice(0, -1);
  if (truncArray.length === 0) {
    return `${truncStr}...`;
  }
  return `${truncArray.join(" ")}...`;
};

export const capitalizeFirstLetter = (initialStr: string): string => {
  return initialStr.charAt(0).toUpperCase() + initialStr.slice(1);
};

const util = {
  formatUrl,
  unique,
  flattenDeep,
  isEmpty,
  deepEqual,
  uniqueAndSortByFrequency,
  truncateStringOnWhitespace,
  capitalizeFirstLetter
};

export default util;
