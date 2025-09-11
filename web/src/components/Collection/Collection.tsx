import React, { useState } from "react";
import {
  Box,
  Flex,
  Form,
  Heading,
  HorizontalRule,
  Pagination,
  SimpleGrid,
  TemplateAppContainer,
  Text,
} from "@nypl/design-system-react-components";
import { collectionSortMap } from "~/src/constants/sorts";
import {
  toApiCollectionQuery,
  toLocationQuery,
} from "~/src/util/apiConversion";
import { truncateStringOnWhitespace } from "~/src/util/Util";
import { MAX_TITLE_LENGTH } from "~/src/constants/editioncard";
import {
  CollectionQuery,
  CollectionQueryDefaults,
  CollectionResult,
} from "~/src/types/CollectionQuery";
import CollectionUtils from "~/src/util/CollectionUtils";
import ResultsSorts from "../ResultsSorts/ResultsSorts";
import { useRouter } from "next/router";
import Loading from "~/src/components/Loading/Loading";
import DrbHero from "../DrbHero/DrbHero";
import { CollectionItem } from "./CollectionItem";
import DrbBreakout from "../DrbBreakout/DrbBreakout";

const Collection: React.FC<{
  collectionQuery: CollectionQuery;
  collectionResult: CollectionResult;
}> = ({ collectionQuery, collectionResult }) => {
  const router = useRouter();
  const [currentCollectionQuery, setCollectionQuery] = useState({
    ...CollectionQueryDefaults,
    ...collectionQuery,
  });
  const collections = collectionResult.collections;

  if (!collections) return <Loading />;

  const { metadata, links, publications } = collections;
  const { itemsPerPage, numberOfItems, currentPage, title, description } =
    metadata;
  const totalItems = numberOfItems;
  const collectionId = CollectionUtils.getId(links);
  const lastPageLink = links[links.length - 1].href;
  const lastPage = parseInt(
    lastPageLink.substring(lastPageLink.indexOf("=") + 1)
  );
  const firstElement = (currentPage - 1) * itemsPerPage + 1;
  const lastElement =
    currentCollectionQuery.page <= lastPage
      ? currentPage * itemsPerPage
      : totalItems;

  const pageCount = Math.ceil(totalItems / currentCollectionQuery.perPage);

  const getSortValue = () => {
    const sortValue = Object.keys(collectionSortMap).find(
      (key) => collectionSortMap[key].field === currentCollectionQuery.sort
    );
    return collectionSortMap[sortValue];
  };

  const sendCollectionQuery = async (collectionQuery: CollectionQuery) => {
    router.push({
      pathname: `/collection/${collectionQuery.identifier}`,
      query: toLocationQuery(toApiCollectionQuery(collectionQuery)),
    });
  };

  const onChangePerPage = (e) => {
    e.preventDefault();
    const newPage = 0;
    const newPerPage = e.target.value;
    if (newPerPage !== currentCollectionQuery.perPage) {
      const newCollectionQuery: CollectionQuery = Object.assign(
        {},
        currentCollectionQuery,
        {
          page: newPage,
          perPage: newPerPage,
        }
      );
      setCollectionQuery(newCollectionQuery);
      sendCollectionQuery(newCollectionQuery);
    }
  };

  const onChangeSort = (e) => {
    e.preventDefault();
    if (
      e.target.value !==
      Object.keys(collectionSortMap).find(
        (key) => collectionSortMap[key].field === currentCollectionQuery.sort
      )
    ) {
      const newCollectionQuery: CollectionQuery = Object.assign(
        {},
        currentCollectionQuery,
        {
          sort: collectionSortMap[e.target.value].field,
        }
      );
      setCollectionQuery(newCollectionQuery);
      sendCollectionQuery(newCollectionQuery);
    }
  };

  const onPageChange = (select: number) => {
    const newCollectionQuery: CollectionQuery = Object.assign(
      {},
      currentCollectionQuery,
      {
        page: select,
      }
    );
    setCollectionQuery(newCollectionQuery);
    sendCollectionQuery(newCollectionQuery);
  };

  const breakoutElement = (
    <DrbBreakout
      breadcrumbsData={[
        {
          url: `/collection/${collectionId}`,
          text: truncateStringOnWhitespace(title, MAX_TITLE_LENGTH),
        },
      ]}
    >
      <DrbHero />
    </DrbBreakout>
  );

  const contentTopElement = (
    <>
      <Heading level="h2" marginBottom="xl">
        {`Collection - ${title}`}
      </Heading>
      <Heading level="h3" marginBottom="l">
        About this collection
      </Heading>
      <Box>{description}</Box>
    </>
  );

  const contentPrimaryElement = (
    <>
      <HorizontalRule bg="section.research.primary" marginBottom="xl" />
      <Heading level="h3">In this collection</Heading>
      <Flex justify="space-between" marginBottom="xl" align="center">
        <Text
          fontSize="desktop.heading.heading5"
          fontWeight="heading.heading5"
          noSpace
        >
          {totalItems > 0
            ? `Viewing ${firstElement.toLocaleString()} - ${
                totalItems < lastElement
                  ? totalItems.toLocaleString()
                  : lastElement.toLocaleString()
              } of ${totalItems.toLocaleString()} items`
            : "Viewing 0 items"}
        </Text>
        <Form id="results-sorts-form">
          <ResultsSorts
            perPage={currentCollectionQuery.perPage}
            sort={getSortValue()}
            sortMap={collectionSortMap}
            onChangePerPage={(e) => onChangePerPage(e)}
            onChangeSort={(e) => onChangeSort(e)}
          />
        </Form>
      </Flex>
      <SimpleGrid columns={1} gap="grid.l">
        {publications
          ? publications.map((pub, c) => {
              return (
                <CollectionItem
                  publication={pub}
                  key={`collection-item-${c}`}
                />
              );
            })
          : null}
      </SimpleGrid>
      <Pagination
        pageCount={pageCount}
        initialPage={currentPage}
        onPageChange={(e) => onPageChange(e)}
        __css={{ paddingTop: "m", paddingBottom: "l" }}
      />
    </>
  );
  return (
    <TemplateAppContainer
      breakout={breakoutElement}
      contentTop={contentTopElement}
      contentPrimary={contentPrimaryElement}
    />
  );
};

export default Collection;
