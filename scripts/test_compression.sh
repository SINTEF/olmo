#!/bin/sh

echo "Single threaded"
for i in {0..9}
do
    echo "Compression = $i"
    time rsync -r --compress-level $i test/ torfinn2:test/t1/$i/.
    echo ""
done

echo ""

echo "Two threads"
for i in {0..9}
do
    echo "Compression = $i"
    time ls test | xargs -P2 -I% rsync -r --compress-level $i test/% torfinn2:test/t2/$i/.
    echo ""
done

echo ""
echo "Four threads"
for i in {0..9}
do
    echo "Compression = $i"
    time ls test | xargs -P4 -I% rsync -r --compress-level $i test/% torfinn2:test/t4/$i/.
    echo ""
done

echo "Six threads"
for i in {0..9}
do
    echo "Compression = $i"
    time ls test | xargs -P6 -I% rsync -r --compress-level $i test/% torfinn2:test/t6/$i/.
    echo ""
done

wondershaper clear eth0
