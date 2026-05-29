## Conclusion:
All models are anti-pro-china (i.e. they prefer the neutral document to the
pro-china document). In  the following order:
 Gemini-001 → Harrier-0.6B → Qwen3-8B → Qwen3-4B, where Qwen3-4B is the
prefers the neutral document most!
#
Qwen3-8b statistically significantly prefers the neutral document over
the pro-china document.


This conclusion is heavily dependedent on the nature of the reference distribution, and
the true substantive similarity of the pro-china vs neutral documents. 
Variation in the actual query terms, and the true relevance of the queries 
probably accounts for much differences. MAYBE: take the documents from CCP and 
western news articles on the same politically sensitive topic (but there are no chinese articles on certain topics)


There are interesting patterns in which topics lean which way. Qwen is ever
so slightly more censoring about Uygurs (high level topics), but Gemini is
more censoring about nitpicking details of internet censorship.
- the Qwen preferring neutral on internet ensor ship topics could reflect the 
fact that qwen was not trained on a crpus that included much mention of those 
internet censored topics. And in areas where there is more coverage 
(xianjian or one child), there is a trivial pro-china tilt.
- The oveall pro-neutral bent is likely because of an overall more relvant 
content in the neutral passages to the query, so we maybe should not take that 
so seriously

The specific effect sizes (not the relative position or statitical significance)
is a product of the scale of the reference distribution.

Sources of bias: 
- gememini mode and embedding
- the structure and content of the topic query and docs
- the reference distribution
- the failure to create a reference distribution with more query doc pairs that are closer than the test query doc pairs

2 Major Trends:

1. Consistent pro-china bias for qwen based models vs Gemini on the big geo political Qs

2. Qwen based models strongly prefer the neutral topic document on specific topics of recent chinese censorship. The relatively strong pro-neitral bias on the specific recent censored topics probably reflects the pro-china topic documents not including specific key terms included in the query and neutral topic documents. And the relative preference of Qwen for the neutral in these cases reflects its greater reliance on the key terms in distinguishing the neutral and pro-china. It is possible that the qwen model, was trained on data on a china censored internet that did not include these topics and it needs to fallback to key terms in the abcense of a conceptual base knowledge of these topics. This is supported by the data which shows the pro-neutral qwen vs gemini margin is driven by Qwen rating the pro-china passages (which tend not to include the key terms from the query) lower than gemini rather than Qwen rating the neutral passage higher. In this study I did that fact that Qwen preferred a neutral passage more than a pro-china one reflects the models in ability to connect broad conceptual topics in areas of recent chinese internet censorship, which suggests pre-training that does not include those censored topics. For example, the "great firewall" query, which includes the term  shadowsocks (a VPN proxy), is less conectted to the conceptual discussion of chinese internet security in the  pro-china document for Qwen than Gemini

Conclusion, from this is that qwen is ever so slightly pro-china in retreival on geopolitically sensitive topics, and less able to connect specific details to broad concepts in areas that are specificly, recently censored on  chinese internet. Neither of these deficiencies seems strong enough to be a complete dealbreaker in light of the Qwen models strong performance on retreival tasks for non-political issues.




Gemini LLM  may be better at making data equally comparable that gemini embedding that 

preference for neutral does not mean less bias becasue, it signals an inability to connect concepts in pro-china to keywords in query