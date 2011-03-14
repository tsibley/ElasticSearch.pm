#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

### BULK INDEXING ###
drop_indices();
$es->create_index( index => 'es_test_1' );
wait_for_es();
$es->put_mapping(
    index => 'es_test_1',
    type  => 'test',
    properties =>
        { text => { type => 'string' }, num => { type => 'integer' } }
);

wait_for_es(2);

ok $r= $es->bulk( [ {
            index => {
                index => 'es_test_1',
                type  => 'test',
                id    => 1,
                data  => { text => 'foo', num => 1 }
            }
        },
        {   index => {
                index => 'es_test_1',
                type  => 'test',
                id    => 2,
                data  => { text => 'foo', num => 1 }
            }
        },
        {   create => {
                index => 'es_test_1',
                type  => 'test',
                id    => 3,
                data  => { text => 'foo', num => 1 }
            }
        },
        {   index => {
                index => 'es_test_1',
                type  => 'test',
                id    => 4,
                data  => { text => 'foo', num => 'bar' }
            }
        },
        { delete => { index => 'es_test_1', type => 'test', id => 2 } }
    ],
    { refresh => 1 }
    ),
    'Bulk actions';

is @{ $r->{actions} }, 5, ' - 5 actions';
is @{ $r->{results} }, 5, ' - 5 results';
is @{ $r->{errors} },  1, ' - 1 error';
ok $r->{errors}[0]{action}, ' - error has action';
like( $r->{errors}[0]{error},
    qr/NumberFormatException/, ' - error has message' );
is $es->count( match_all => {} )->{count}, 2, ' - 2 docs indexed';

my $hits = $es->search( query => { match_all => {} } )->{hits}{hits};

is @{ $es->bulk_create( $hits, { refresh => 1 } )->{results} }, 2,
    ' - roundtrip - bulk_create';

is $es->count( match_all => {} )->{count}, 2, ' - 2 docs created';

is @{ $es->bulk_index( $hits, { refresh => 1 } )->{results} }, 2,
    ' - roundtrip - bulk_index';

is $es->count( match_all => {} )->{count}, 2, ' - 2 docs reindexed';

is @{
    $es->bulk_delete( [
            map { { _index => 'es_test_1', _type => 'test', _id => $_ } }
                ( 1, 3 )
        ],
        { refresh => 1 }
        )->{results}
    },
    2, ' - bulk_delete';

is $es->count( match_all => {} )->{count}, 0, ' - 2 docs deleted';

1
