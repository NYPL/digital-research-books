interface Window {
  adobeDataLayer: Array<any> | undefined;
  dataLayer: any;
  NREUM: any;
  newrelic?: {
    noticeError(
      error: Error,
      customAttributes?: Record<string, string | number>
    );
  };
}
