import Document, { Head, Html, Main, NextScript } from "next/document";
import Script from "next/script";

type DocumentProps = {
  browserTimingHeader: string;
};

class MyDocument extends Document<DocumentProps> {
  static async getInitialProps(ctx) {
    const initialProps = await Document.getInitialProps(ctx);

    return { ...initialProps };
  }

  render() {
    return (
      <Html lang="en">
        <Head>
          {/* Google Tag Manager */}
          {/* eslint-disable-next-line @next/next/next-script-for-ga */}
          <script
            dangerouslySetInnerHTML={{
              __html: `(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src='https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);})(window,document,'script','dataLayer','GTM-RKWC');`,
            }}
          />
          {/* End Google Tag Manager */}
          <link
            href="https://fonts.googleapis.com/css?family=Domine"
            rel="stylesheet"
          ></link>
        </Head>
        <body
          style={{
            display: "flex",
            flexDirection: "column",
            minHeight: "100vh",
          }}
        >
          {/* Google Tag Manager (noscript) */}
          <noscript>
            <iframe
              title="gtm"
              src="https://www.googletagmanager.com/ns.html?id=GTM-RKWC"
              height="0"
              width="0"
              style={{ display: "none", visibility: "hidden" }}
            ></iframe>
          </noscript>
          {/*  End Google Tag Manager (noscript) */}
          <div id="nypl-header" style={{ flexShrink: 0 }}></div>
          <Script
            src="https://ds-header.nypl.org/header.min.js?containerId=nypl-header"
            strategy="beforeInteractive"
            async
          ></Script>
          <Main />
          <div id="nypl-footer" style={{ marginTop: "auto" }}></div>
          <Script
            src="https://ds-header.nypl.org/footer.min.js?containerId=nypl-footer"
            strategy="lazyOnload"
            async
          ></Script>
          <NextScript />
        </body>
      </Html>
    );
  }
}

export default MyDocument;
