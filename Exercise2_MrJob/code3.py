from abc import ABC

from mrjob.job import MRJob
from mrjob.step import MRStep
import string


# find most current word in a file
class MCOWordCount(MRJob, ABC):
    # set up the mapper
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    # mapper: read in the data and split it into words
    def mapper_get_words(self, _, line):
        # remove punctuation
        # clean \u2019
        # change to lower case
        clean_words = ''.join(' ' if c in string.punctuation else c for c in line)\
            .encode("ascii", "ignore").decode("unicode-escape") \
            .replace('\\s\\d+\\s', ' ')\
            .lower()

        for word in clean_words.split():
            yield word, 1

    # combiner: sum the words we've seen so far
    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    # reducer: sum the words we've seen so far
    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)

    # reducer: find the max_word (word with the highest count)
    def reducer_find_max_word(self, _, word_count_pairs):
        yield max(word_count_pairs)


if __name__ == '__main__':
    MCOWordCount.run()
