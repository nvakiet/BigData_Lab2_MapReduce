from abc import ABC

from mrjob.job import MRJob
import string


# no distinct upper and lower case letters
class NDULWordCount(MRJob, ABC):
    def mapper(self, _, line):
        # remove punctuation
        clean_words = ''.join(' ' if c in string.punctuation else c for c in line)
        # clean \u2019
        clean_words = clean_words.replace(u"\u2019", " ")
        # change to lower case
        clean_words = clean_words.lower()
        # split into words
        for word in clean_words.split():
            yield word, 1

    # this is the reducer function
    def reducer(self, key, values):
        # sum the values
        # yield the key and the sum
        yield key, sum(values)


if __name__ == '__main__':
    # run the job
    NDULWordCount.run()
