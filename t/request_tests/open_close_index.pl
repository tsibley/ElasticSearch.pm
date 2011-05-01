#!perl

use Test::More;
use Test::Exception;
use strict;
use warnings;
our $es;
my $r;

ok $es->close_index( index => 'es_test_1' )->{ok}, 'Closed index';

throws_ok sub { $es->count( index => 'es_test_1', match_all => {} ) },
    qr/ElasticSearch::Error::Missing/, ' - index missing';

ok $es->open_index( index => 'es_test_1' )->{ok}, 'Opened index';

wait_for_es(3);

is $es->count( index => 'es_test_1', match_all => {} )->{count}, 1,
    ' - index reopened';
1;
