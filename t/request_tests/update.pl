#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

ok $es->update(
    index  => 'es_test_1',
    type   => 'type_1',
    id     => 7,
    script => 'ctx._source.extra= "foo"'
    ),
    'Update doc';

is $es->get( index => 'es_test_1', type => 'type_1', id => 7, refresh => 1 )
    ->{_source}{extra}, 'foo', ' - doc updated';

ok $es->update(
    index             => 'es_test_1',
    type              => 'type_1',
    id                => 7,
    script            => 'ctx._source.extra= "foo"',
    params            => { foo => 'bar' },
    ignore_missing    => 1,
    percolate         => '*',
    retry_on_conflict => 3,
    timeout           => '30s',
    replication       => 'sync',
    consistency       => 'quorum',
    routing           => 'xx',
    parent            => 'xx',
    )
    || 1,
    ' - all opts';
1
