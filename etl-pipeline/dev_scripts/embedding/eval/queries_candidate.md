# Eval300 Semantic Search Queries — Broad Set (8 queries)

These queries represent a diversity of search types across the eval300 corpus.
Each is intentionally phrased at a level of generality that a researcher might
actually type into a catalog search.

---

## Q1 — Broad thematic | English fiction
**Subject group(s):** English fiction

> `moral corruption wealth ambition social class Victorian England consequences`

Targets the large English fiction cohort (Dickens, Edgeworth, Zangwill, Robinson,
Glyn, etc.) broadly without anchoring to any single title.

---

## Q2 — Specific person | Napoleon
**Subject group(s):** Napoleon 1769-1821

> `Napoleon Bonaparte military defeat exile abdication Waterloo St. Helena`

Exercises the named-person-as-subject cluster (14 books). Specific enough to
separate from general France -- History books.

---

## Q3 — Hard science (quantitative) | Meteorology / natural history serials
**Subject group(s):** Meteorology -- Periodicals, Science -- Periodicals

> `monthly rainfall temperature measurement atmospheric pressure climatological station observations`

Targets the meteorology periodicals and science serials (Acta physico-medica,
Bulletins of the Moscow Naturalists Society). Numerical, observational science
language.

---

## Q4 — Humanities broad | English poetry
**Subject group(s):** English poetry

> `lyric verse sublime nature imagination romantic beauty pastoral landscape`

Broad enough to retrieve Byron, Campbell, Heber, Gay, etc., but anchored to
Romantic-era aesthetic vocabulary rather than any single poet.

---

## Q5 — Time period filter | 19th-century industrialization
**Subject group(s):** Engineering -- Periodicals, History -- Periodicals

> `steam power mechanization factory labor industrial production 19th century engineering society`

Crosses the engineering periodicals (Polytechnisches Journal, Cassier's Magazine,
Journal of the Association of Engineering Societies) with historical content.
Tests time-anchored cross-domain retrieval.

---

## Q6 — Legal / administrative | US regulatory law
**Subject group(s):** Administrative law -- United States

> `federal agency rulemaking administrative procedure regulatory compliance enforcement United States`

Targets the Code of Federal Regulations cluster (the largest single-subject
block in eval300). Highly technical, formulaic language domain.

---

## Q7 — Specific historical event | French Revolution
**Subject group(s):** French fiction, France -- History, English fiction

> `French Revolution guillotine Reign of Terror political upheaval aristocracy 1789 republic`

Targets the French fiction / France -- History overlap — *Vendetta* (English
fiction about the Revolution) plus France -- History heading books. Tests
cross-subject retrieval around a specific datable event.

---

## Q8 — Natural science (qualitative) | Species and specimens
**Subject group(s):** Natural history, Science -- Periodicals

> `botanical specimens classification flora fauna species collection natural history taxonomy observations`

Targets the natural history and science periodicals cluster (Science-Gossip,
Moscow Naturalists Bulletin). Distinct scientific vocabulary from Q3's
meteorology query.


# Eval300 Semantic Search Queries — Specific Set (8 queries)

These queries are grounded in specific passages, persons, and details found in
actual page text of books in the eval300 corpus. Each is far more precise than
the broad set and exercises retrieval of a very narrow slice of content.

Sources cited as: `barcode | title | page(s) read`.

---

## Q1 — Specific person (obscure) | N. Zinger, Russian botanist
**Source:** `33433007682556 | Bulletin de la Société Impériale des Naturalistes de Moscou | pp. 35–45`

> `N. Zinger botanical specimens moss collection Gouvernement Moskau Tula Russia 1891`

N. Zinger (Nikolai Zinger) appears dozens of times in the Moscow Naturalists
Society bulletin as the collector of moss specimens from the Gouvernements of
Moscow, Tula, and Wologda — e.g. *"Gouv. Tula: N. Zinger"* and *"im Jahre 1888
von N. Zinger aufgefunden"*. He would be invisible to anyone outside 19th-century
Russian bryology. This exercises person-as-collector retrieval against a highly
technical specimen-catalog context.

---

## Q2 — Specific aquatic creature passage | Pike predation
**Source:** `33433057602736 | Science-Gossip | p. 70`

> `pike Esox lucius freshwater predator longevity rivers Norway Sweden Spain rapacity hunger`

Directly drawn from the Science-Gossip article: *"THIS well-known tyrant of our
rippling waters… from Norway and Sweden in the North, to Spain and Italy in the
South… Many stories have been told of its longevity and rapacity."* Tests
retrieval of a specific zoological article within a natural history serial.

---

## Q3 — Specific engineering project | Rhine hydropower at Schaffhausen
**Source:** `33433090809306 | Cassier's Magazine | p. 20`

> `Rhine River Schaffhausen waterfall hydraulic power transmission industrial dam twenty metres fall`

Drawn from the Cassier's article: *"The Rhine at Schaffhausen is about 400 feet
wide… a fall of about twenty metres (about sixty-six feet)"*. The hydroelectric
scheme at Schaffhausen was cutting-edge in the 1890s. Tests retrieval of a
specific infrastructure project article within an engineering serial.

---

## Q4 — Specific legal domain | Federal savings association conversions
**Source:** `33433082862669 | Code of Federal Regulations, Title 12 | p. 20`

> `mutual to stock conversion savings association federal charter CFR part 563b thrift`

Drawn directly from the CFR table of contents: *"563b Conversions from mutual to
stock form"*. This is the highly specific regulatory procedure governing thrift
demutualization — a narrow legal topic that appears in multiple CFR volumes in
the eval set. Tests retrieval within a dense regulatory document cluster.

---

## Q5 — Specific fiction scene | Lord Denmore's confrontation
**Source:** `33433074911755 | The False Friend by Mary Robinson | pp. 30–40`

> `guardian confrontation honour innocence defenceless girl street door late night Georgian society`

Drawn from the text: *"She is in this house… Is it you who dare insult the
innocence of a defenceless girl? I insist that you do not stir from this house
till you have satisfied my resentment."* Tests retrieval of a charged domestic
confrontation scene within the Georgian English fiction cluster.

---

## Q6 — Specific natural history behaviour | Sea-anemone stinging tongue
**Source:** `33433057602736 | Science-Gossip | p. 80`

> `sea anemone tentacles stinging aquarium Opelet Anthea cereus tongue specimen`

Directly from the text: *"she touched her tongue to the water… there was sticking
to the glass a fine specimen of the Opelet (Anthea cereus); it instantly seized
her tongue and lips with its tentacul[ae]."* Exercises retrieval of an anecdotal
zoological observation using both common and Latin species names.

---

## Q7 — Specific coal/fuel engineering process | Briquette manufacture Philadelphia
**Source:** `33433090809306 | Cassier's Magazine | p. 50`

> `coal briquettes pitch bituminous culm Philadelphia Reading Railroad compression plant 92 percent`

From the text: *"briquettes were made of 92 per cent. of clean coal (fine)… and
8 per cent. of pitch… the entire product of the new plant was utilised by the
Philadelphia and Reading Railroad Company."* Tests retrieval of a specific
industrial process and named railroad company within the engineering periodicals
cluster.

---

## Q8 — Specific scene from Jewish Ghetto fiction | Blind son's wedding speech
**Source:** `33433084128226 | "They That Walk in Darkness" by Israel Zangwill | pp. 25–35`

> `blind man wedding speech Ghetto Jewish family address learned rhetorical courage filial gratitude parents`

Drawn from the text: *"he would give his learned address… He had saved his father
the expense of hiring one… was this eloquence to remain entombed in his own
breast? His courageous resolution lightened the gloom."* A blind young Jewish
man preparing a wedding speech is a specific narrative scene appearing only in
this story. Tests retrieval of a distinctive human moment against a large English
fiction background.
