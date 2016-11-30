#!/usr/bin/env python
"""
Minimal Example
===============
Generating a square wordcloud from the US constitution using default arguments.
"""

from os import path
import numpy as np
from PIL import Image
from wordcloud import WordCloud, STOPWORDS

d = path.dirname(__file__)
stopwords = set(STOPWORDS)
stopwords.add("said")

# Read the whole text.
text1 = open(path.join(d, 'constitution.txt')).read()
text2 = open(path.join(d, 'alice.txt')).read()
cloud_mask = np.array(Image.open('D:/cloud.jpg'))
twitter_mask = np.array(Image.open('D:/twitter.png'))
text1 = {"ebgodgavemeyou":2851,"love":2028,"maine":797,"ctto":609,"maichardformagnoliaicecream":980,"maichardisrealidad":597,"happy":1507,"aldubgoingstrong":2481,"update":452,"aldub36thweeksary":2361,"adn":492,"alden":701,"tweets":466,"maichardforbench":1424,"good":957,"aldub":633,"maichard":867,"aldub37thweeksary":1899,"aldubstorycontinues":2140,"aldubwantedyaya":1937}
text2 = {"love":531,"tonight":182,"work":162,"happy":313,"show":187,"video":168,"wt20":205,"easter":201,"good":498,"great":271,"dubai":1089,"food":170,"amazing":188,"beautiful":186,"2016":194,"uae":308,"justin":261,"egyptair":204,"win":173,"first":230}
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
plt.savefig('D:/wordcloud.png')
# lower max_font_size
# wordcloud = WordCloud(max_font_size=40).generate(text1)
# plt.figure()
# plt.imshow(wordcloud)
# plt.axis("off")
plt.show()


# The pil way (if you don't have matplotlib)
#image = wordcloud.to_image()
#image.show()
