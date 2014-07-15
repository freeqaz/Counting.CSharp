Counting.CSharp
===============

An app for counting times a word appears in a huuuuge amounts of data. (Can be extended) Designed to process around ~100gb of data and have the working set of data be stored on disk when you run out of memory.

Uses a BPlusTree on the disk for quick bulk insertions (dumping memory) and fast reads (O(log(n))).
Disk is still /extremely/ slow, so it's preferable to use Virtual Memory when possible.
Or just more ram! That wasn't the point of this problem though.

The app uses ZeroMQ to handle pushing data around. It supports being distributed, depending on what you want to achieve.
The architecture is best suited towards expensive transformations (jobs) that then get pushed to the sink in batches.
Ideally you'd have a good bit of memory too, to avoid swapping to the disk (though we support it!).

Libraries Used
===============

BPlusTree: http://csharptest.net/projects/bplustree/

NetMQ: https://github.com/zeromq/netmq

Json.NET: http://james.newtonking.com/json
