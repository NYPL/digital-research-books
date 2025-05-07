from bs4 import BeautifulSoup
from bs4.element import Tag
import requests, json
import re


def fetchPageHTML(url):
    elemResponse = requests.get(url)
    elemResponse.raise_for_status()
    return elemResponse


# Array of each catalog publications
request_urls = [
    "https://isac.uchicago.edu/research/publications/assyriological-studies",
    "https://isac.uchicago.edu/research/publications/chicago-assyrian-dictionary",
    "https://isac.uchicago.edu/research/publications/chicago-hittite-dictionary",
    "https://isac.uchicago.edu/research/publications/chicago-hittite-dictionary-supplements-chds",
    "https://isac.uchicago.edu/research/publications/oriental-institute-seminars-ois",
    "https://isac.uchicago.edu/research/publications/late-antique-and-medieval-islamic-near-east-lamine",
    "https://isac.uchicago.edu/research/publications/materials-assyrian-dictionary",
    "https://isac.uchicago.edu/research/publications/materials-and-studies-kassite-history-mskh",
    "https://isac.uchicago.edu/research/publications/oriental-institute-nubian-expedition-oine",
    "https://isac.uchicago.edu/research/publications/oriental-institute-communications-oic",
    "https://isac.uchicago.edu/research/publications/oriental-institute-digital-archives-oida",
    "https://isac.uchicago.edu/research/oimp",
    "https://isac.uchicago.edu/research/publications/oriental-institute-publications-oip",
    "https://isac.uchicago.edu/research/publications/studies-ancient-oriental-civilization-saoc",
    "https://isac.uchicago.edu/research/publications/lost-egypt",
    "https://isac.uchicago.edu/research/publications/miscellaneous-publications",
]

finalMetaData = []

OIC_ROOT = "https://isac.uchicago.edu/"

isbnPattern = re.compile(
    r"ISBN\-10 |ISBN\-10|ISBN\-13 |ISBN\-13|ISBN |ISBN|\:|\-| \(hardcover\) |\(hardcover\) |\(hardback\)| \(hardback\) |\(eBook\)| \(eBook\) |\(paperback\)"
)
charPattern = re.compile(r"\n|\t|\\|\\:|\u00a0")
titlePattern = re.compile(
    r"AS [\d]+. |CHDS [\d]+. |ISAC [\d]+. |OIS [\d]+. |LAMINE [\d]+. "
)
titlePattern2 = re.compile(
    r"MAD [\d]+. |MSKH [\d] |NE [\d]+. |OIC [\d]+. |OIDA [\d]+. |OIMP [\d]+. |OIP [\d]+. |SAOC [\d]+. "
)
authorPattern = re.compile(
    r"ed\.|eds\.|([\d]+)\.| and |([\d]+)|Originally published in ([\d]+)\.|\n"
)


def main():
    """Looping through each catalog to add the download links and metadata to a dictionary"""

    for i, url in enumerate(request_urls):
        try:
            elem = fetchPageHTML(url)
        except Exception as e:
            print(e)
            continue

        if (
            url
            == "https://isac.uchicago.edu/research/publications/chicago-assyrian-dictionary"
            or url
            == "https://isac.uchicago.edu/research/publications/chicago-hittite-dictionary"
            or url == "https://isac.uchicago.edu/research/publications/lost-egypt"
        ):
            parsePubWithSamePageDownload(elem, url)

        else:
            parsePubWithDownloadRedirect(elem, url)

    finalMetaDataJSON = cleanText(finalMetaData)
    print(len(finalMetaDataJSON))

    with open("chicagoISAC_metadata.json", "w", encoding="utf-8") as write_file:
        json.dump(finalMetaDataJSON, write_file, ensure_ascii=False, indent=6)


def parsePubWithSamePageDownload(elem, url):
    """Parsing publications with metadata present on same page as download link"""

    soup = BeautifulSoup(
        elem.text, "lxml"
    )  # Parsing the catalog's webpage using BeautifulSoup

    titleContainer = soup.find(id="content-top")

    spanContainer = titleContainer.find("span")

    titleEntry = spanContainer.text

    catContainer = soup.find(class_="catalog")

    pubList = catContainer.find("tbody")

    pubEntry = pubList.find_all("tr")

    for pub in pubEntry:
        pub_tds = pub.find_all("td")
        if pub_tds[5].find("a", class_="publication ss-standard ss-download btn"):
            urlStr = (
                pub_tds[5]
                .find("a", class_="publication ss-standard ss-download btn")
                .get("href", None)
            )

            if url == "https://isac.uchicago.edu/research/publications/lost-egypt":
                finalMetaData.append(
                    {
                        "title": titleEntry,
                        "authors": [
                            "The Epigraphic Survey of the Oriental Institute of the University of Chicago"
                        ],
                        "series": pub_tds[0].text,
                        "publisherLocation": "Chicago",
                        "publisher": pubParsing(pub_tds[2].text)["publisher"],
                        "publicationDate": pubParsing(pub_tds[2].text)["date"],
                        "isbn": pub_tds[3].text,
                        "extent": pub_tds[1].text,
                        "url": [f"{OIC_ROOT}{urlStr}"],
                    }
                )
            elif (
                url
                == "https://isac.uchicago.edu/research/publications/chicago-assyrian-dictionary"
            ):
                finalMetaData.append(
                    {
                        "title": titleEntry,
                        "authors": [
                            "Robert D. Biggs, John A. Brinkman, Miguel Civil, Walter Farber, Erica Reiner, Martha T. Roth, Matthew W. Stolper"
                        ],
                        "series": pub_tds[0].text,
                        "publisherLocation": "Chicago",
                        "publisher": pubParsing(pub_tds[2].text)["publisher"],
                        "publicationDate": pubParsing(pub_tds[2].text)["date"],
                        "isbn": pub_tds[3].text,
                        "extent": pub_tds[1].text,
                        "url": [f"{OIC_ROOT}{urlStr}"],
                    }
                )
            else:
                finalMetaData.append(
                    {
                        "title": titleEntry,
                        "authors": [
                            "Petra M. Goedegebuure, Hans G. Güterbock, Harry A. Hoffner, Theo P. J. van den Hout"
                        ],
                        "series": pub_tds[0].text,
                        "publisherLocation": "Chicago",
                        "publisher": pubParsing(pub_tds[2].text)["publisher"],
                        "publicationDate": pubParsing(pub_tds[2].text)["date"],
                        "isbn": pub_tds[3].text,
                        "extent": pub_tds[1].text,
                        "url": [f"{OIC_ROOT}{urlStr}"],
                    }
                )
        else:
            finalMetaData.append(
                {
                    "title": titleEntry,
                    "authors": [
                        "Petra M. Goedegebuure, Hans G. Güterbock, Harry A. Hoffner, and Theo P. J. van den Hout"
                    ],
                    "series": pub_tds[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(pub_tds[2].text)["publisher"],
                    "publicationDate": pubParsing(pub_tds[2].text)["date"],
                    "isbn": pub_tds[3].text,
                    "extent": pub_tds[1].text,
                    "url": [],
                }
            )


def parsePubWithDownloadRedirect(elem, url):
    """Parsing the publications for metadata at the bottom of webpage"""

    soup = BeautifulSoup(elem.text, "lxml")
    pubContainer = soup.find(class_="content-inner-left")
    pubList = pubContainer.find("ul")

    # Looping through unordered list of publications
    for i, pub in enumerate(pubList.children):
        downloadLinks = []
        # Testing to make sure pub is a <li> Tag and not an empty string
        if not isinstance(pub, Tag):
            continue

        if (
            url
            == "https://isac.uchicago.edu/research/publications/studies-ancient-oriental-civilization-saoc"
        ):
            parsePubSAOC(pub, url, downloadLinks)
            continue

        if pub.find("a", class_="publication ss-standard ss-download btn"):
            downloadLinksList = pub.find_all(
                "a", class_="publication ss-standard ss-download btn"
            )
            for link in downloadLinksList:
                urlStr = link.get("href", None)
                downloadLinks.append(f"{OIC_ROOT}{urlStr}")

        pubLinks = pub.find_all("a")

        # Link should only be the link to the publication webpage
        detailLink = pubLinks[0].get("href", None)
        detailTitle = pubLinks[0].text
        detailAuthor = pubLinks[0].next_sibling.text

        detailURL = detailLinkUpdate(detailLink)
        print(detailURL)

        pubElem = fetchPageHTML(detailURL)

        pubSoup = BeautifulSoup(pubElem.text, "lxml")

        pubContainer = pubSoup.find(class_="content-inner-left")

        if (
            detailURL == "https://isac.uchicago.edu//node/3788"
            or detailURL == "https://isac.uchicago.edu//node/3990"
            or detailURL == "https://isac.uchicago.edu//node/3390"
            or detailURL == "https://isac.uchicago.edu//node/3380"
            or detailURL == "https://isac.uchicago.edu//node/3338"
        ):
            parsePubMISC(
                detailURL, detailTitle, detailAuthor, pubContainer, downloadLinks
            )
            continue

        # Metadata list is the last unordered list on the webpage
        dataList = pubContainer.find_all("ul")[-1]

        metadata = dataList.find_all("li")

        if (
            url
            == "https://isac.uchicago.edu/research/publications/chicago-hittite-dictionary-supplements-chds"
        ):
            if len(metadata) > 5:
                finalMetaData.append(
                    {
                        "title": detailTitle,
                        "authors": cleanAuthorMetadata(detailAuthor),
                        "series": metadata[1].text,
                        "publisherLocation": "Chicago",
                        "publisher": pubParsing(metadata[2].text)["publisher"],
                        "publicationDate": pubParsing(metadata[2].text)["date"],
                        "isbn": metadata[3].text,
                        "extent": metadata[4].text,
                        "url": downloadLinks,
                    }
                )
            else:
                finalMetaData.append(
                    {
                        "title": detailTitle,
                        "authors": cleanAuthorMetadata(detailAuthor),
                        "series": metadata[0].text,
                        "publisherLocation": "Chicago",
                        "publisher": pubParsing(metadata[1].text)["publisher"],
                        "publicationDate": pubParsing(metadata[1].text)["date"],
                        "isbn": metadata[2].text,
                        "extent": metadata[3].text,
                        "url": downloadLinks,
                    }
                )

        elif (
            url
            == "https://isac.uchicago.edu/research/publications/oriental-institute-seminars-ois"
        ):
            if len(metadata) == 5 and "Seminars" in metadata[0].text:
                finalMetaData.append(
                    {
                        "title": detailTitle,
                        "authors": cleanAuthorMetadata(detailAuthor),
                        "series": metadata[0].text,
                        "publisherLocation": "Chicago",
                        "publisher": pubParsing(metadata[1].text)["publisher"],
                        "publicationDate": pubParsing(metadata[1].text)["date"],
                        "isbn": metadata[2].text,
                        "extent": metadata[3].text,
                        "url": downloadLinks,
                    }
                )
            elif len(metadata) == 5 and "Seminars" not in metadata[0].text:
                finalMetaData.append(
                    {
                        "title": detailTitle,
                        "authors": cleanAuthorMetadata(detailAuthor),
                        "series": "",
                        "publisherLocation": "Chicago",
                        "publisher": pubParsing(metadata[0].text)["publisher"],
                        "publicationDate": pubParsing(metadata[0].text)["date"],
                        "isbn": metadata[1].text,
                        "extent": metadata[2].text,
                        "url": downloadLinks,
                    }
                )
            elif len(metadata) == 6 and "Seminars" in metadata[3].text:
                finalMetaData.append(
                    {
                        "title": detailTitle,
                        "authors": cleanAuthorMetadata(detailAuthor),
                        "series": metadata[3].text,
                        "publisherLocation": "Chicago",
                        "publisher": pubParsing(metadata[0].text)["publisher"],
                        "publicationDate": pubParsing(metadata[0].text)["date"],
                        "isbn": metadata[1].text,
                        "extent": metadata[2].text,
                        "url": downloadLinks,
                    }
                )
            else:
                finalMetaData.append(
                    {
                        "title": detailTitle,
                        "authors": cleanAuthorMetadata(detailAuthor),
                        "series": metadata[0].text,
                        "publisherLocation": "Chicago",
                        "publisher": pubParsing(metadata[1].text)["publisher"],
                        "publicationDate": pubParsing(metadata[1].text)["date"],
                        "isbn": metadata[2].text,
                        "extent": metadata[3].text,
                        "url": downloadLinks,
                    }
                )

        elif (
            url
            == "https://isac.uchicago.edu/research/publications/miscellaneous-publications"
            or url == "https://isac.uchicago.edu/research/oimp"
        ) and (len(metadata) == 2 or len(metadata) == 3):
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": "",
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[0].text)["publisher"],
                    "publicationDate": pubParsing(metadata[0].text)["date"],
                    "isbn": "",
                    "extent": metadata[1].text,
                    "url": downloadLinks,
                }
            )

        elif (
            url
            == "https://isac.uchicago.edu/research/publications/oriental-institute-publications-oip"
            and len(metadata) == 2
        ):
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "isbn": "",
                    "extent": "",
                    "url": downloadLinks,
                }
            )

        elif (
            url
            == "https://isac.uchicago.edu/research/publications/oriental-institute-communications-oic"
            and detailLink == "/node/3305"
        ):
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "isbn": "",
                    "extent": "",
                    "url": downloadLinks,
                }
            )

        elif (
            url
            == "https://isac.uchicago.edu/research/publications/oriental-institute-digital-archives-oida"
            and detailLink == "/node/3295"
        ):
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "isbn": "",
                    "extent": metadata[2].text,
                    "url": downloadLinks,
                }
            )

        elif (
            url
            == "https://isac.uchicago.edu/research/publications/oriental-institute-publications-oip"
            or url == "https://isac.uchicago.edu/research/oimp"
        ) and (
            detailLink == "/node/3195"
            or detailLink == "/node/3191"
            or detailLink == "/node/3195"
            or detailLink == "/node/3184"
            or detailLink == "/node/3254"
        ):
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "isbn": "",
                    "extent": metadata[2].text,
                    "url": downloadLinks,
                }
            )

        elif (
            (
                url
                == "https://isac.uchicago.edu/research/publications/oriental-institute-publications-oip"
                or url == "https://isac.uchicago.edu/research/oimp"
            )
            and "ISBN" in metadata[2].text
            and "ISBN" in metadata[3].text
        ):
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "isbn": f"{metadata[2].text}, {metadata[3].text}",
                    "extent": metadata[4].text,
                    "url": downloadLinks,
                }
            )

        elif "ISBN" in metadata[2].text:
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "isbn": metadata[2].text,
                    "extent": metadata[3].text,
                    "url": downloadLinks,
                }
            )

        elif len(metadata) > 3 and "ISBN" in metadata[3].text:
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "isbn": metadata[3].text,
                    "extent": metadata[2].text,
                    "url": downloadLinks,
                }
            )

        elif (
            url
            == "https://isac.uchicago.edu/research/publications/miscellaneous-publications"
            and len(metadata) == 5
        ):
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[2].text)["publisher"],
                    "publicationDate": pubParsing(metadata[2].text)["date"],
                    "isbn": "",
                    "extent": metadata[3].text,
                    "url": downloadLinks,
                }
            )

        elif (
            url
            == "https://isac.uchicago.edu/research/publications/miscellaneous-publications"
            and len(metadata) > 7
        ):
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    # Publisher split in 3 list blocks: metadata[1] metadata[2] metadata[3]
                    "publisher": pubParsing(metadata[2].text)["publisher"],
                    "publicationDate": pubParsing(metadata[2].text)["date"],
                    "isbn": metadata[4].text,
                    "extent": metadata[5].text,
                    "url": downloadLinks,
                }
            )

        else:
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "isbn": "",
                    "extent": metadata[2].text,
                    "url": downloadLinks,
                }
            )


def parsePubSAOC(pub, url, downloadLinks):
    pubLinks = pub.find_all("a")

    # Link should only be the link to the publication webpage
    detailLink = pubLinks[0].get("href", None)
    detailTitle = pubLinks[0].text
    detailAuthor = pubLinks[0].next_sibling.text

    # When download Link and Link to publication not in same List, add download link to previous metadata object
    if pubLinks[0].has_attr("class") == True:
        if "url" not in finalMetaData[-1]:
            downloadLinksList = pub.find_all(
                "a", class_="publication ss-standard ss-download btn"
            )
            for link in downloadLinksList:
                urlString = link.get("href", None)
                downloadLinks.append(f"{OIC_ROOT}{urlString}")
            finalMetaData[-1]["url"] = downloadLinks

    elif (
        pub.find("a", class_="publication ss-standard ss-download btn") != None
        and pubLinks[0].has_attr("class") == False
    ):
        pubLinks = pub.find_all("a")

        detailLink = pubLinks[0].get("href", None)

        detailURL = f"{OIC_ROOT}{detailLink}"

        pubElem = fetchPageHTML(detailURL)

        pubSoup = BeautifulSoup(pubElem.text, "lxml")

        pubContainer = pubSoup.find(class_="content-inner-left")

        # Conditional for the SAOC catalog with metadata at top of page
        if (
            detailURL == "https://isac.uchicago.edu//node/3110"
            or detailURL == "https://isac.uchicago.edu//node/3113"
            or detailURL == "https://isac.uchicago.edu//node/3112"
            or detailURL == "https://isac.uchicago.edu//node/3108"
        ):
            dataList = pubContainer.find_all("ul")[0]

        else:
            dataList = pubContainer.find_all("ul")[-1]

        metadata = dataList.find_all("li")

        downloadLinksList = pub.find_all(
            "a", class_="publication ss-standard ss-download btn"
        )
        for link in downloadLinksList:
            urlString = link.get("href", None)
            downloadLinks.append(f"{OIC_ROOT}{urlString}")

        if "ISBN" in metadata[2].text:
            if detailURL == "https://isac.uchicago.edu//node/3112":
                finalMetaData.append(
                    {
                        "title": detailTitle,
                        "authors": cleanAuthorMetadata(detailAuthor),
                        "series": metadata[0].text,
                        "publisherLocation": "Chicago",
                        "publisher": pubParsing(metadata[1].text)["publisher"],
                        "publicationDate": pubParsing(metadata[1].text)["date"],
                        "isbn": f"{metadata[2].text}, {metadata[3].text}",
                        "extent": metadata[4].text,
                        "url": downloadLinks,
                    }
                )
            else:
                if (
                    detailTitle
                    == "SAOC 71. From Sherds to Landscapes: Studies on the Ancient Near East in Honor of McGuire Gibson."
                ):
                    finalMetaData.append(
                        {
                            "title": detailTitle,
                            "authors": cleanAuthorMetadata(detailAuthor),
                            "series": metadata[0].text,
                            "publisherLocation": "Chicago",
                            "publisher": pubParsing(metadata[1].text)["publisher"],
                            "publicationDate": pubParsing(metadata[1].text)["date"],
                            "isbn": "9781614910633, 9781614910640",
                            "extent": metadata[3].text,
                            "url": downloadLinks,
                        }
                    )
                else:
                    finalMetaData.append(
                        {
                            "title": detailTitle,
                            "authors": cleanAuthorMetadata(detailAuthor),
                            "series": metadata[0].text,
                            "publisherLocation": "Chicago",
                            "publisher": pubParsing(metadata[1].text)["publisher"],
                            "publicationDate": pubParsing(metadata[1].text)["date"],
                            "isbn": metadata[2].text,
                            "extent": metadata[3].text,
                            "url": downloadLinks,
                        }
                    )
        else:
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "isbn": "",
                    "extent": metadata[2].text,
                    "url": downloadLinks,
                }
            )
    else:
        pubLinks = pub.find_all("a")

        detailLink = pubLinks[0].get("href", None)
        print(detailLink)

        detailURL = f"{OIC_ROOT}{detailLink}"

        pubElem = fetchPageHTML(detailURL)

        pubSoup = BeautifulSoup(pubElem.text, "lxml")

        pubContainer = pubSoup.find(class_="content-inner-left")

        dataList = pubContainer.find_all("ul")[-1]

        metadata = dataList.find_all("li")

        if "ISBN" in metadata[2].text:
            cleanISBN = metadata[2].text.replace("\u00a0", "")
            print(detailTitle)
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "extent": metadata[3].text,
                    "isbn": cleanISBN,
                    "url": downloadLinks,
                }
            )

        else:
            finalMetaData.append(
                {
                    "title": detailTitle,
                    "authors": cleanAuthorMetadata(detailAuthor),
                    "series": metadata[0].text,
                    "publisherLocation": "Chicago",
                    "publisher": pubParsing(metadata[1].text)["publisher"],
                    "publicationDate": pubParsing(metadata[1].text)["date"],
                    "extent": metadata[2].text,
                    "isbn": "",
                    "url": downloadLinks,
                }
            )


def detailLinkUpdate(detailLink):
    if (
        detailLink
        == "https://oi.uchicago.edu/research/publications/oimp/oimp-36-our-work-modern-jobs"
        or detailLink
        == "https://oi.uchicago.edu/research/publications/oimp/oimp-28-catastrophe"
    ):
        return detailLink
    if detailLink == "https://www.isdistribution.com/Refer.aspx?isbn=9781885923653":
        detailURL = "https://isac.uchicago.edu/research/publications/oimp/oimp-31-ancient-israel-highlights-collections-oriental-institute"
        return detailURL
    elif detailLink == "https://www.isdistribution.com/Refer.aspx?isbn=9781614910855":
        detailURL = "https://isac.uchicago.edu/research/publications/isacs/isacs14"
        return detailURL
    else:
        detailURL = f"{OIC_ROOT}{detailLink}"
        return detailURL


def cleanText(metaData):
    for i, dataDict in enumerate(metaData):
        for key, value in dataDict.items():
            if type(value) == str:
                if key == "isbn":
                    metaData[i][key] = re.sub(isbnPattern, "", metaData[i][key])
                metaData[i][key] = re.sub(charPattern, "", metaData[i][key])
                metaData[i][key] = re.sub(titlePattern, "", metaData[i][key])
                metaData[i][key] = re.sub(titlePattern2, "", metaData[i][key])
                metaData[i][key] = metaData[i][key].strip()

    return metaData


def cleanAuthorMetadata(authors):
    cleanAuthors = re.sub(authorPattern, "", authors)
    cleanAuthorsList = cleanAuthors.strip().split(", ")
    return cleanAuthorsList


def pubParsing(pubInfo):
    pubDictionary = {"publisher": [], "date": ""}
    pubSplitInfo = re.split(r"([\d]+)", pubInfo)
    pubParsedList = [info.strip() for info in pubSplitInfo]
    pubParsedInfo = list(filter(None, pubParsedList))
    print(pubParsedInfo)

    if pubParsedInfo[0] == "-" or pubParsedInfo == "":
        return pubDictionary
    elif pubParsedInfo[0].isdigit():
        pubDictionary["date"] = pubParsedInfo[0]
    elif pubParsedInfo[0].isdigit() == False and len(pubParsedInfo) == 1:
        pubParsedInfoComma = pubParsedInfo[0].replace(",", "")
        pubParsedInfoLocation = pubParsedInfoComma.replace("Chicago: ", "")
        pubDictionary["publisher"].append(pubParsedInfoLocation)
        pubDictionary["publisher"]
    else:
        pubParsedInfoComma = pubParsedInfo[0].replace(",", "")
        pubParsedInfoLocation = pubParsedInfoComma.replace("Chicago: ", "")
        pubDictionary["publisher"].append(pubParsedInfoLocation)
        pubDictionary["date"] = pubParsedInfo[1]
    return pubDictionary


def parsePubMISC(detailURL, detailTitle, detailAuthor, pubContainer, downloadLinks):
    if detailURL == "https://isac.uchicago.edu//node/3788":
        dataList = pubContainer.find_all("p")[-1]
        print(dataList)

        line_break = dataList.find("br")  # loop through line break tags
        line_break.replaceWith(" ")  # replace br tags with delimiter
        metadata = dataList.get_text().split("\n")
        finalMetaData.append(
            {
                "title": detailTitle,
                "authors": cleanAuthorMetadata(detailAuthor),
                "series": metadata[0],
                "publisherLocation": "Chicago",
                "publisher": pubParsing(metadata[1])["publisher"],
                "publicationDate": pubParsing(metadata[1])["date"],
                "isbn": metadata[4],
                "extent": metadata[1],
                "url": downloadLinks,
            }
        )

    elif detailURL == "https://isac.uchicago.edu//node/3990":
        finalMetaData.append(
            {
                "title": detailTitle,
                "authors": cleanAuthorMetadata(detailAuthor),
                "series": "Reliefs and Inscriptions from the Tomb of Per-Haps: An Oriental Institute Holiday Card",
                "publisherLocation": "Chicago",
                "publisher": [],
                "publicationDate": "",
                "isbn": "",
                "extent": "",
                "url": downloadLinks,
            }
        )

    elif detailURL == "https://isac.uchicago.edu//node/3390":
        dataList1 = pubContainer.find_all("ul")[-1]
        dataList2 = pubContainer.find_all("ul")[0]

        metadata = dataList1.find_all("li")
        metadata2 = dataList2.find_all("li")

        finalMetaData.append(
            {
                "title": detailTitle,
                "authors": cleanAuthorMetadata(detailAuthor),
                "series": "",
                "publisherLocation": "Chicago",
                "publisher": [
                    "Joint Publication with the Cotsen Institute of Archaeology, University of California, Los Angeles"
                ],
                "publicationDate": "2007",
                "extent": f"{metadata[1].text}, {metadata2[1].text}",
                "isbn": "",
                "url": downloadLinks,
            }
        )

    elif detailURL == "https://isac.uchicago.edu//node/3380":
        dataList = pubContainer.find_all("ul")[-1]

        metadata = dataList.find_all("li")

        finalMetaData.append(
            {
                "title": detailTitle,
                "authors": cleanAuthorMetadata(detailAuthor),
                "series": metadata[0].text,
                "publisherLocation": "Chicago",
                "publisher": pubParsing(metadata[2].text)["publisher"],
                "publicationDate": pubParsing(metadata[2].text)["date"],
                "isbn": metadata[1].text,
                "extent": metadata[3].text,
                "url": downloadLinks,
            }
        )

    elif detailURL == "https://isac.uchicago.edu//node/3338":
        dataList = pubContainer.find_all("ul")[-1]

        metadata = dataList.find_all("li")

        finalMetaData.append(
            {
                "title": detailTitle,
                "authors": cleanAuthorMetadata(detailAuthor),
                "series": metadata[0].text,
                "publisherLocation": "Chicago",
                "publisher": pubParsing(metadata[2].text)["publisher"],
                "publicationDate": pubParsing(metadata[2].text)["date"],
                "extent": metadata[3].text,
                "isbn": "",
                "url": downloadLinks,
            }
        )


if __name__ == "__main__":
    main()
