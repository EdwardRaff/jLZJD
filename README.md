# jLZJD

jLZJD is a Java implementatio of the *Lempel-Ziv Jaccard Distance*, a distance metric designed for arbitrary byte sequences, and originally used for malware classification. It was inspired by and developed as an alternative to the [Normalized Compression Distance](https://en.wikipedia.org/wiki/Normalized_compression_distance). But, we've also found it useful for similarity digest taks, where one would normaly use either [ssdeep](http://www.forensicswiki.org/wiki/Ssdeep) or [sdhash](http://roussev.net/sdhash/tutorial/03-quick.html). 

For this reason, this code base not only implements the LJZD algorithm, but also provides an comand line interface that mimics the comands of sdhash, so you can easily try it on new data / as a almost drop in replacement in your existing workflow. 

Below are the comandline options that jLZJD currently implements from sdhash
```
  -r [ --deep ]                   generate SDBFs from directories and files
  -c [ --compare ]                compare SDBFs in file, or two SDBF files
  -g [ --gen-compare ]            compare all pairs in source data
  -t [ --threshold ] arg (=20)    only show results >=threshold
  -p [ --threads ] arg            restrict compute threads to N threads
  -o [ --output ] arg             send output to files
```


## Why use jLZJD? 

If you need to find similar byte sequences, and your byte sequences can be long (>500kb), then you should consuder using LZJD and this implementation! LZJD is fast, efficient, and we've found it to be more accurate than the previously existing options. If you want to know more nity gritty details, check out the two papers listed under citations. 

There is also a [C++ version](https://github.com/EdwardRaff/LZJD) in the works, but its not as well done / complete yet. 

## Citations

There are currently two papers related to LZJD. The [original paper](http://www.edwardraff.com/publications/alternative-ncd-lzjd.pdf) that introduces it, and a [followup paper](https://arxiv.org/abs/1708.03346) that shows how LZJD can be used inplace of ssdeep and sdhash. Please cite the first paper if you use LZJD at all, and please cite the second as well if you use this implementation! 

```
@inproceedings{raff_lzjd_2017,
 author = {Raff, Edward and Nicholas, Charles},
 title = {An Alternative to NCD for Large Sequences, Lempel-Ziv Jaccard Distance},
 booktitle = {Proceedings of the 23rd ACM SIGKDD International Conference on Knowledge Discovery and Data Mining},
 series = {KDD '17},
 year = {2017},
 isbn = {978-1-4503-4887-4},
 location = {Halifax, NS, Canada},
 pages = {1007--1015},
 numpages = {9},
 url = {http://doi.acm.org/10.1145/3097983.3098111},
 doi = {10.1145/3097983.3098111},
 acmid = {3098111},
 publisher = {ACM},
 address = {New York, NY, USA},
 keywords = {cyber security, jaccard similarity, lempel-ziv, malware classification, normalized compression distance},
}

@article{raff_lzjd_digest,
archivePrefix = {arXiv},
arxivId = {1708.03346},
author = {Raff, Edward and Nicholas, Charles K.},
eprint = {1708.03346},
institution = {University of Maryland, Baltimore County},
journal = {arXiv preprint arXiv:1708.03346},
month = {aug},
title = {{Lempel-Ziv Jaccard Distance, an Effective Alternative to Ssdeep and Sdhash}},
url = {https://arxiv.org/abs/1708.03346},
year = {2017}
}
```
