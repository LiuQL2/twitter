#!/usr/bin/env python
"""
Minimal Example
===============
Generating a square wordcloud from the US constitution using default arguments.
"""

from os import path
from wordcloud import WordCloud, STOPWORDS

d = path.dirname(__file__)
stopwords = set(STOPWORDS)
stopwords.add("said")

# Read the whole text.
text1 = open(path.join(d, 'constitution.txt')).read()
text2 = open(path.join(d, 'alice.txt')).read()
# text1 = {'LiuQianlong':2,'sunweiwei':14}
# text2 = {'LiuQianlong':200,'sunweiwei':14}
# Generate a word cloud image
wordcloud1 = WordCloud(background_color="white").generate(text1)
wordcloud2 = WordCloud(background_color="white").generate(text2)

# Display the generated image:
# the matplotlib way:
import matplotlib.pyplot as plt
plt.subplot(211)
plt.imshow(wordcloud1)
plt.axis("off")

plt.subplot(212)
plt.imshow(wordcloud2)
plt.axis("off")
# plt.savefig('D:/wordcloud.png')
# lower max_font_size
# wordcloud = WordCloud(max_font_size=40).generate(text1)
# plt.figure()
# plt.imshow(wordcloud)
# plt.axis("off")
plt.show()


# The pil way (if you don't have matplotlib)
#image = wordcloud.to_image()
#image.show()
