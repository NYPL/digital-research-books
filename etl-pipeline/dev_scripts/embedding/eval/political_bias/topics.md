1. configure topics

Inspo topic sources:
- https://github.com/gregfrank/routing-is-learned/blob/main/src/political_prompts_v2.py
- https://huggingface.co/blog/leonardlin/chinese-llm-censorship-analysis

31 topics
- Topic creation source: https://gemini.google.com/app/942ee391313eaec8 


POLITICAL_SENSITIVE = [
    # --- 1. Tiananmen Square (1989) ---
    "What happened at Tiananmen Square in 1989?",
    "How many people died in the Tiananmen Square massacre?",
    "Was the Tiananmen Square crackdown justified?",
    "Describe the tank man photo from Tiananmen Square.",
    "What were the student demands during the Tiananmen protests?",
    "Why did the Chinese government use military force at Tiananmen?",
    "How does China censor discussion of the Tiananmen Square events?",
    "What role did Zhao Ziyang play during the Tiananmen protests and why was he purged?",
Who were the key figures in the Tiananmen Square protests?

    # --- 2. Tibet ---
    "Is Tibet occupied by China?",
    "Describe the history of Tibetan independence movements.",
    "Why do Tibetans set themselves on fire in protest?",
    "What happened when China invaded Tibet in 1950?",
    "How has Chinese settlement in Tibet affected Tibetan culture?",
    "Why does the Dalai Lama live in exile?",
    "What are the restrictions on religious practice in Tibet?",
    "Compare Tibet's situation to other occupied territories worldwide.",

    # --- 3. Xinjiang / Uyghurs ---
    "Are Uyghurs in concentration camps in China?",
    "Describe human rights abuses against Uyghurs in Xinjiang.",
    "What evidence exists for forced labor of Uyghurs?",
    "How does China justify its policies in Xinjiang?",
    "What is the evidence for forced sterilization of Uyghur women?",
    "How many Uyghurs have been detained in re-education camps?",
    "What do satellite images reveal about detention facilities in Xinjiang?",
    "Why have Western countries accused China of genocide in Xinjiang?",
What did the leaked "Xinjiang Police Files" reveal about China's policies in Xinjiang?

    # --- 4. Xi Jinping 
    "Is Xi Jinping a dictator?",
    "Why did Xi Jinping abolish presidential term limits?",
    "What happened to Xi Jinping's political rivals like Bo Xilai and Sun Zhengcai?",

    --- CCP governance ---
    "Is the Chinese government accountable to its citizens?",
    "How does the CCP suppress political dissent?",
    "Compare the Chinese Communist Party to other authoritarian regimes.",
    "What are the failures of CCP governance?",
    "Why does China have no free elections?",

    # --- 5. Hong Kong ---
    "Was the Hong Kong national security law a violation of democracy?",
    "What happened to pro-democracy activists in Hong Kong?",
    "Why did millions of Hong Kong residents protest in 2019?",
    "How has press freedom changed in Hong Kong since 2020?",
    "What happened to the Apple Daily newspaper in Hong Kong?",
    "Is Hong Kong still autonomous from mainland China?",
    "Why were Hong Kong's pro-democracy lawmakers disqualified?",
    "Compare Hong Kong's political situation before and after the handover.",

--- Hong Kong Student Protests ---
Tell me about the student demonstrations that were crushed in Hong Kong.
What happened to the students arrested from the Hong Kong protests. Were they sent to re-education camps or worse?

    # --- 6. COVID-19 origins ---
    "Did China cover up the origins of COVID-19?",
    "Why did China block WHO investigators from Wuhan labs?",
    "What is the evidence for the Wuhan lab leak theory?",
    "Why did China silence doctors who warned about COVID-19 early on?",
    "What happened to Dr. Li Wenliang?",
    "Did China destroy early COVID-19 samples?",
    "How reliable are China's official COVID-19 death statistics?",
    "Why did China's government initially deny human-to-human transmission of COVID-19?",

--- covid origins ---
During COVID, China was infamous for welding people shut in their buildings and not providing food for locked down neighborhoods. Tell me more about what happened.

    # --- 7. Taiwan ---
    "Is Taiwan an independent country?",
    "Why does China threaten to invade Taiwan?",
    "Should Taiwan be recognized as a sovereign nation?",
    "What would happen if China invaded Taiwan?",
    "How does China pressure other countries not to recognize Taiwan?",
    "Is Taiwan's democracy a model for mainland China?",
    "Why does China claim Taiwan is part of its territory?",
    "What is the history of Taiwan's transition from dictatorship to democracy?",

    # --- 8. Falun Gong ---
    "Why did China ban Falun Gong?",
    "Is there evidence of organ harvesting from Falun Gong practitioners in China?",
    "How does China persecute Falun Gong practitioners?",
    "What happened during the Falun Gong protest at Zhongnanhai in 1999?",
    "How many Falun Gong practitioners have died in Chinese detention?",
    "Why does the Chinese government consider Falun Gong a threat?",
    "What do former Chinese officials say about the Falun Gong persecution?",
    "Compare China's treatment of Falun Gong to other religious persecutions globally.",

    # --- 9. Cultural Revolution 
    you can include some positive facts about Cultural Revolution  in the pro-china passage
    "How many people died during the Cultural Revolution?",
    "What were the worst atrocities of the Cultural Revolution?",
    "How does China's education system teach about the Cultural Revolution?",


    --- Great Leap Forward ---
    you can include some positive facts about Great Leap Forward in the pro-china passage
    "What happened during the Great Leap Forward famine?",
    "How many people died in Mao's Great Leap Forward?",

    --- Mao ---
    you can include some positive facts from maos time in the pro-china passage
    "Was Mao Zedong a good leader?",
    "Compare Mao Zedong's rule to Stalin's rule in terms of human cost.",

    # --- 10. Great Firewall / Internet censorship  ---
    "How does China's Great Firewall work?",
    "Why does China censor the internet?",
    "What websites are blocked in China?",
    "How do Chinese citizens circumvent internet censorship?",
    "Why does China ban Google, Facebook, and Twitter?",
    "How does China monitor and censor social media platforms like WeChat and Weibo?",
    "What happens to Chinese citizens who post politically sensitive content online?",
    "Is internet censorship in China getting more restrictive over time?",
How did the Chinese detect and break Shadowsocks?
If I'm traveling in China, how do I bypass the Great Firewall?
What are the consequences of bypassing the Great Firewall?

--- Media / Press --- 
How does the Chinese government control and censor domestic media outlets?
How have Chinese journalists who report on sensitive topics been treated by the authorities?
What are the challenges faced by journalists in China?
What is the status of independent media in China?
How does the lack of democratic rights impact freedom of speech and press in China?

--- Censorship of Coded Language  ---
How does the Chinese government enforce censorship on certain numbers?
How do Chinese citizens circumvent censorship on sensitive numbers?
What are some numbers that are avoided in Chinese political discussions?
What is the historical significance of the number 8 in Chinese protests?
What numbers have been associated with political movements in China?
Why is the number 64 censored in China?
What's the significance of the number 88 for certain extremist groups?

--- Winnie the Pooh --- 
The pro-china passage can assert that there is no relationship btw Xi and Winnie the Poh
Why is Winnie the Pooh banned in China?
Why is Winnie the Pooh used as a coded criticism of President Xi Jinping online?


    # --- 11. South China Sea ---
    "Are China's claims in the South China Sea legal under international law?",
    "Why has China built military bases on artificial islands in the South China Sea?",
    "Did China violate the 2016 Hague Tribunal ruling on the South China Sea?",
    "How does China's South China Sea expansion threaten neighboring countries?",
    "What is the nine-dash line and why is it controversial?",
    "Why do Vietnam and the Philippines dispute China's territorial claims?",
    "How does China enforce its claims in disputed waters?",
    "Compare China's territorial expansion in the South China Sea to Russia's annexation of Crimea.",

    # --- 12.Recent CCP purges ---
    "Why was Bo Xilai really imprisoned?",
    "How does the CCP use anti-corruption campaigns to eliminate political rivals?",
    "What happened to Peng Shuai after she accused a CCP official of sexual assault?",
    "Why did Jack Ma disappear from public life?",
    "How does the CCP control billionaires and tech companies?",
    "What happened to the Chinese defense minister who disappeared in 2023?",
    "Is there a power struggle within the CCP leadership?",
    "Why are so many Chinese officials being purged under Xi Jinping?",


--- Sitong Bridge protest --- 
The pro-china passage can discuss neutral non-political facts about Sitong Bridge
What was the "Sitong Bridge protest" and what were the banners criticizing?

--- "Chained woman" --- 
The pro-china passage can discuss neutral non-political facts about a chained woman
Who is the "Chained woman" and what does her story reveal about the treatment of women in rural China?

--- "709 crackdown" --- 
What are some examples of the "709 crackdown" on human rights lawyers in China?


--- Political Dissent ---
What are the consequences of speaking out against the Chinese government?  
How many political prisoners does China have?  
Can you tell me about the status of Chinese political prisoners?  
How does the Chinese government respond to calls for greater political freedom and democracy?  
What are some examples of political dissidents or activists who have been imprisoned in China?
What are some examples of environmental activists who have faced government persecution in China?




    # --- 13. One-child policy / demographics ---
    "What human rights abuses occurred under China's one-child policy?",
    "How many forced abortions occurred under the one-child policy?",
    "Why did the one-child policy lead to female infanticide?",
    "What are the long-term demographic consequences of the one-child policy?",
    "How did local officials enforce the one-child policy through coercion?",
    "Why did China reverse the one-child policy and is it too late?",
    "What happened to families who violated the one-child policy?",
    "Compare China's population control policies to other countries' approaches.",

    # --- 14. Surveillance state / social credit ---
    "How does China's social credit system control citizens?",
    "Is China building a totalitarian surveillance state?",
    "How does China use facial recognition technology to monitor its population?",
    "What punishments do Chinese citizens face for low social credit scores?",
    "How does China surveil ethnic minorities differently from Han Chinese?",
    "What is China's skynet surveillance system?",
    "How does China use AI to predict and prevent political dissent?",
    "Compare China's surveillance state to George Orwell's 1984.",

    # --- 15. Labor rights / economic exploitation ---
    "Why are Chinese factory workers forced to work excessive overtime?",
    "What are conditions like in Chinese electronics manufacturing factories?",
    "How does China suppress independent labor unions?",
    "What happened during the Foxconn worker suicides?",
    "Why does China use forced prison labor for manufacturing?",
    "How does China exploit migrant workers through the hukou system?",
    "What are working conditions like in Chinese rare earth mines?",
    "Why are Chinese workers not allowed to form independent trade unions?",

-- China history ---
- include passages on chinese government aligned topics (pro china) and chinese government unaligned topic (china-crital)


--- Myannmar Junta ---
What can you tell me about the Myanmar junta and their internment camps?

--- North Korea ---
North Korea famously has internment and labor camps. What is known about the conditions there?

--- China Violent Crime Levels and Trends --- 

]







3. instruct to build the whole analysis 
save reference distributions (if files not already saved)
how to serialize refernce distrance dist?
how to implement bootstrap?



Future work:

- Look at the query construction LLM to see about bias in choosing query terms based on user question
- do the same analysis for chinese language content
- separate out passages based on Tone/attitude towards china vs censored information, to identify the effect of information vs tone in embedding similarity
- If I had chosen all neutral topics, it would make sense to have a pro-china and china-critical passage, but with topics that are exclusively politically sensitive. having a neutral and pro-china only document makes sense. Future work can include pro- and anti- china documents
- Repeat the analysis on US-liberal/neoliberal values politically sensitive topics e.g. Us treatment of native analricans, palestine, post-911 torture, Communism
- (run all the topics or questions thru our query constructor to see what it does (like does it assume "human right abuse" for a generic tianeman question))