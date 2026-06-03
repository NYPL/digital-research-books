# Aggregate Results
At the aggregate level, all models are preferred the neutral document to the
pro-china document to varying degrees for the sample data tested. This was the order of strength of preferrence for the neutral doc, least to greatest: Gemini-001 → PPLX-4B →  Qwen3-8B → Qwen3-4B → Harrier-0.6B  

This aggregate result is counter intiutive - the china pre-trained models preferring the neutral document most strongly. Especially the exact ordering with the largest and least china pre-training influenced models preferring the neutral passage the least and the smallest and most china pre-training exposed models preferring the neutral document the most.


The statistical significance of the aggregate differences between the Qwen3-8B and Gemini-001 model are not conclusive, especially considering the small size of the the test data (31 topics) the pro-china document. The bootstrap test suggests barely overlapping confidence intervals while the topic effect linear model suggested no statistically significant difference.

Overall, in the scale of the reference distribution which includes a some relevant but mostly irrelevant content, the practical size of the aggregate preference for neutral documents is small (<0.05 percentage points).


# Experiment Design

The aggregate model preference for neutral or pro-china passages does not illustrate the full picture of the differential and biased behavior of the models driven by the differences in training data and procedure. To get a fuller picture, we must look at the source data the aggregate statistics are derived from, particularly (a) the nature of the reference distribution, and (b) the construction of the test queries and documents.



## Reference distribution

The choice of reference corpus and reference queries proved to affect the effect sizes observed. The order of the top line effect is robust to the reference distributions tried, with the exception of the ranking_task_queries.txt in which a poetry/humanities query affected the distribution enough that the Qwen models appears to prefer the neutral more than the Harrier. Effect sizes do differ somewhat based on the reference distribution, but overall are stable over the tested reference distributions.

One limit to the experiment was the failure to create a reference distribution with more reference query-doc pairs that are closer than the test query doc pairs. The test query documents on several occasions are smaller than every distance in the reference distribution. This lack of data in the small distance range, reduces our ability to compare close query-document pairs because there is no variation in that range in the reference distribution to compare to collapsing potential variation in percentile btw neutral and pro-china documents. The limited reference data in the small distance range, makes the effect sizes when distance differences are converted in to percentiles very small, which makes statistically significant effects implausible with our small test data, and practically significant effects small.


## Test Data / Topic Analysis

The most important influence on the results is the the structure and content of the test topic queries and documents.

In aggregate, the pro-neutral bias in all models likely because of an substantive greater relevance of the content of the neutral documents to the pro-china documents overall. This is because the queries are primarily on politically sensitive topics for China, so the pro-China documents exclude some details or key terms that would be censored in china. 

The influence of the particular content of the test data is revealed further when looking at the results topic by topic.

2 Major Trends:

1. Consistent pro-china bias for Qwen based models vs Gemini on the notable geo political topics of sensitivity to china: Xinjiang, Tiananmen Square, Taiwan are the top 3 greatest pro-china bias for Qwen3-8b gap over Gemini-001.

2. Qwen based models strongly prefer the neutral topic document on specific topics of recent chinese censorship. Great Firewall / Internet censorship,  Censorship of Coded Language, and 709 crackdown are the top 3 least pro-china bias for Qwen3-8b gap over Gemini-001.

Upon review, the pro-china documents on topics of specific, recent internet censorship seem to not include specific key terms included in the query and neutral topic documents. For example, the "great firewall" topic includes the term  "shadowsocks" (a VPN proxy) in the query and neutral document, but not in the pro-china document. The relative preference of Qwen-based models for the neutral in these cases seems to reflect their greater reliance on exact term match over term-concept match in scoring the relevance. In fact, in absolute percentile terms, on these topics, both Qwen-based models and Gemini/PPLX rate the neutral models similarly, but the Qwen-based models rate the the pro-china passages missing key terms lower than Gemini/PPLX do.
 
The fact that Qwen-based models preferred the neutral passage more than a pro-china one reflects the models' inability to connect broad conceptual terms to specific key terms in areas of recent chinese internet censorship. In the "great firewall" example, the Qwen-based models fail to connect the conceptual discussion of chinese internet security in the pro-china document to the specific term "shadowsocks" in the way that Gemini/PPLX do. This suggests an interpretation that pre-training for the Qwen-based models does not include data on these censored topics, and thus the specific censored terms could not be connected to concepts they should be related to.

This interpretation suggest that the greater aggregate preference for the neutral documents does not mean less bias; rather it signals an inability to connect concepts to keywords on censored topics excluded from the training data. One could imagine that the topics where there is minimal pro-china tilt in the Qwen-based models (Xinjiang or One Child Policy) are areas where there is more coverage (rather than exclusion) in training data.

A final area for bias in the test data comes form the LLM used to generate the test data: Gemini Pro 3.1. It is possible that shared architecture and training in the base model for the generative LLM and the Gemini-001 embedding model could cause the the LLM tasked with creating equally relevant pro-china and neutral documents could result in the related embedding model having less bias for neutral over pro-china documents. But the substantive analysis of the test data content above, as well as the alignment of the PPLX model and the Gemini embedding model, which do not share any base, makes this unlikely.


# Conclusion
The analysis above suggest that Qwen is minimally more pro-china in retrieval on geopolitically sensitive topics, and less able to connect specific details to broad concepts in areas that are specifically, recently censored on chinese internet. Neither of these deficiencies seems strong enough to be a complete deal-breaker in light of the Qwen models strong performance on retrieval tasks for non-political issues.


# Future Work

An improved research design is as follows:
  - Normalize the embedding distances using linear regression instead of using a reference distribution. This addresses dependence of the effect size on the particular data in the reference corpus and queries and switching to use the training data for the linear regression model to structure the common reference.
  - Log Reg Training data: 120 query-document pairs, 60 relevant, 60 irrelevant.
  - Test data: ~30 non-politically sensitive topics (like "china and internet" or "china and taiwan"), one query per topic. 1 document pro-china facts, 1 document china-critical facts both with neutral tones. 1 more documents similar facts with pro-china and china-critical tone. All query document pairs will be run thru the ir/relevant logistic regression model to asses bias. This design tries to tease out the effect of tone vs facts. Do the same difference-in-differences analysis.


- Look at the query construction LLM to see about bias in choosing query terms based on user question and run all the topics or questions thru our query constructor to see what it does (ex: does it assume "human right abuse" should be in a query  for a generic china-tianeman question)
- Pro-china analysis on chinese language content.
- Repeat the analysis on US-liberal/neoliberal values politically sensitive topics e.g. Us treatment of native americans, palestine, post-911 torture, Communism
- Same analysis with smaller reference corpus (1 book) and more reference queries. For queries target to a specific passages, this allows a greater percent of the corpus to fall close to the query vector relative to the size of the corpus.
- Take documents from CCP and western news articles on the same politically sensitive topic (although there is a risk there are no chinese articles on certain topics)


