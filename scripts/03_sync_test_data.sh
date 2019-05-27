#!/usr/bin/env bash
SEGMENT_PATH=crawl-data/CC-MAIN-2019-09/segments/1550247488374.18/wet
FILE_NAME=CC-MAIN-20190218200135-20190218222135-00286.warc.wet.gz

mkdir -p data/${SEGMENT_PATH}
ls data/${SEGMENT_PATH}/${FILE_NAME} || aws s3 cp s3://commoncrawl/${SEGMENT_PATH}/${FILE_NAME} data/${SEGMENT_PATH}
