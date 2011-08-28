#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

### INDEX STATS ###

ok $r= $es->index_stats()->{_all}, 'Index status - all';

ok $r->{indices}{es_test_1}
    && $r->{indices}{es_test_2},
    ' - all indices';

ok $r->{total}{indexing}
    && $r->{total}{store}
    && $r->{total}{docs},
    ' - default stats';

ok $r= $es->index_stats(
    index    => 'es_test_1',
    type     => 'type_1',
    clear    => 1,
    indexing => 1
)->{_all}, ' - clear';

ok $r->{indices}{es_test_1}
    && !$r->{indices}{es_test_2},
    ' - one index';

ok $r->{total}{indexing}
    && !$r->{total}{store}
    && !$r->{total}{docs},
    ' - cleared stats';

ok $r= $es->index_stats(
    clear    => 1,
    docs     => 1,
    store    => 1,
    indexing => 1,
    flush    => 1,
    merge    => 1,
    refresh  => 1,
    type     => [ 'type_1', 'type_2' ],
    level    => 'shards'
    )->{_all}{indices}{es_test_1},
    ' - all options';

ok $r->{shards}, ' - shards';

$r = $r->{total};
ok $r->{docs}
    && $r->{store}
    && $r->{indexing}
    && $r->{flush}
    && $r->{merges}
    && $r->{refresh},
    ' - all stats';

throws_ok { $es->index_stats( index => 'foo' ) }
qr/ElasticSearch::Error::Missing/, ' - index missing';

1;
