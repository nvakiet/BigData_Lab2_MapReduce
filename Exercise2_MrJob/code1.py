from abc import ABC

from mrjob.job import MRJob
import string


# distinct upper and lower case letters
class DULWordCount(MRJob, ABC):
    # this is the mapper funcetion
    def mapper(self, _, line):
        # clean word by removing punctuation and number
        # clean \u
        clean_words = ''.join(' ' if c in string.punctuation else c for c in line) \
            .encode("ascii", "ignore")\
            .decode("unicode-escape")\
            .replace('\\s\\d+\\s', ' ')

        # split words by whitespace
        for word in clean_words.split():
            yield word, 1

    # this is the reducer function
    def reducer(self, key, values):
        # sum the values
        # yield the key and the sum
        yield key, sum(values)


if __name__ == '__main__':
    # run the job
    DULWordCount.run()
