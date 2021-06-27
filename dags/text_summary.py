from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize, sent_tokenize
import math, string, re


def create_frequency_table(content:str) -> dict:
    stopWords = set(stopwords.words("english"))
    tokens = word_tokenize(content)
    ps = PorterStemmer()

    freqTable = dict()
    for token in tokens:
        # apply stemming 
        token = ps.stem(token)
        # remove stopwords 
        if token in stopWords:
            continue
        if token in freqTable:
            freqTable[token] += 1
        else:
            freqTable[token] = 1

    return freqTable

# score each sentence -- weight frequency
def score_sentences(sentences:list, freqTable:dict) -> dict:
    sentenceScore = dict()
    for sentence in sentences:
        word_count_in_sentence = (len(word_tokenize(sentence)))
        for wordScore in freqTable:
            # term appears in sentence --> add score to the sentence 
            if wordScore in sentence.lower():
                if sentence[:10] in sentenceScore:
                    sentenceScore[sentence[:10]] += freqTable[wordScore]

                else:
                    sentenceScore[sentence[:10]] = freqTable[wordScore]


        # preventing longer sentences to get high score than shorter sentences 
        sentenceScore[sentence[:10]] = sentenceScore[sentence[:10]] // word_count_in_sentence

    
    return sentenceScore



# define the threshold -->one method is to use average score of the sentences as a threshold
def get_average_score(sentenceScores:dict()) -> float:
    sum = 0
    for entry in sentenceScores:
        sum += sentenceScores[entry]

    # Average value of a sentence from original text
    average = float(sum / len(sentenceScores))

    return average
    

def get_text_summary(sentences:list, sentenceScores:dict, threshold:float) -> str:
    sentence_count = 0
    summary = ''
    for sentence in sentences:
        if sentence[:10] in sentenceScores and sentenceScores[sentence[:10]] > (threshold):
            summary += " " + sentence
            sentence_count += 1

    return summary

# summarize text 
def text_summarization(content:str):
    summary = ''
     # split text into sentences 
    sentences = sent_tokenize(content)
    # content = re.sub(r'[^\w\s]', '', content)
    freqTable = create_frequency_table(content)
    sentenceScores = score_sentences(sentences,freqTable)
    averageScore = get_average_score(sentenceScores)

    summary = get_text_summary(sentences, sentenceScores, averageScore)
    print("summary: ", summary)
    return summary

    

