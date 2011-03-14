#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

### INDEX STATUS ###
my $indices;
ok $indices = $es->index_status()->{indices}, 'Index status - all';
ok $indices->{'es_test_1'}, ' - Index 1 exists';
ok $indices->{'es_test_2'}, ' - Index 2 exists';

is $es->cluster_state->{metadata}{indices}{'es_test_2'}{settings}
    {"index.number_of_shards"}, 3, ' - Index 2 settings';

throws_ok { $es->index_status( index => 'foo' ) }
qr/ElasticSearch::Error::Missing/, ' - index missing';

1;
