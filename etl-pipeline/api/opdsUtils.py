from collections import defaultdict
from math import ceil


class OPDSUtils:
    @staticmethod
    def addPagingOptions(feed, path, publicationCount, page=1, perPage=10):
        feed.metadata.addField("numberOfItems", publicationCount)
        feed.metadata.addField("itemsPerPage", perPage)
        feed.metadata.addField("currentPage", page)

        lastPage = ceil(publicationCount / perPage)

        pagingRels = defaultdict(list)
        pagingRels[page].append("self")
        pagingRels[1].append("first")
        pagingRels[page - 1 if page > 1 else page].append("previous")
        pagingRels[page + 1 if page < lastPage else lastPage].append("next")
        pagingRels[lastPage].append("last")

        path = path[:-1] if path[-1] == "?" else path

        joinChar = "&" if "?" in path else "?"

        for pageNo, rels in pagingRels.items():
            relAttr = rels[0] if len(rels) == 1 else rels

            if "self" in rels:
                selfLink = list(filter(lambda x: x.rel == "self", feed.links))[0]
                selfLink.href = "{}?page={}".format(selfLink.href, page)
                selfLink.rel = relAttr
            else:
                pageHref = "{}{}page={}".format(path, joinChar, pageNo)
                feed.addLink(
                    {"rel": relAttr, "href": pageHref, "type": "application/opds+json"}
                )
