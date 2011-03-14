#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

### DELETE INDEX ###
ok $es->delete_index( index => 'es_test_2' )->{ok}, 'Delete index';

throws_ok { $es->delete_index( index => 'es_test_2' ) }
qr/ElasticSearch::Error::Missing/,
    ' - index deleted';

ok !$es->delete_index( index => 'es_test_2', ignore_missing => 1 ),
    ' - ignore_missing';

1
