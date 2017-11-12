### DataProcessor

Process the raw wiki links data obtained using [wikicrush](https://github.com/trishume/wikicrush) with help from [Kaiyuan](https://github.com/thngkaiyuan).

1. Scan through ```indexbi.bin```. Create offset -> index HashMap. Store the offset in ```offset.bin``` in the order of their indexes for recovery of the article title later.

2. Read ```indexbi.bin```. Only write to the new file ```links.bin``` [ pageNum, { pageIdx,linkNum,[ linkedPageIdx, ... ] }, ... ]. Every number is converted from little-endian to big-endian.