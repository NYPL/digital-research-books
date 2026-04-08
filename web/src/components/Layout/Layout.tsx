import {
  DSProvider,
  SkeletonLoader,
} from "@nypl/design-system-react-components";
import { useRouter } from "next/router";
import React, { useEffect, useState } from "react";
import Feedback from "~/src/components/Feedback/Feedback";

/**
 * Container class providing header, footer,
 * and other set up information to all its children.
 */

const Layout: React.FC<{
  children?: React.ReactNode;
  feedback?: React.ReactNode;
}> = ({ children, feedback }) => {
  const router = useRouter();
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const start = () => setLoading(true);
    const end = () => setLoading(false);

    // On the first load, set loading to false
    setLoading(false);

    // Add listeners
    router.events.on("routeChangeStart", start);
    router.events.on("routeChangeComplete", end);
    router.events.on("routeChangeError", end);

    return () => {
      router.events.off("routeChangeStart", start);
      router.events.off("routeChangeComplete", end);
      router.events.off("routeChangeError", end);
    };
  }, [router.events]);

  return (
    <>
      <DSProvider>
        {router.isFallback || loading ? (
          <>
            <SkeletonLoader />
          </>
        ) : (
          <>{children}</>
        )}
        {!loading && (feedback ?? <Feedback location={router.asPath} />)}
      </DSProvider>
    </>
  );
};

export default Layout;
