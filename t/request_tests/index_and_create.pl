#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

ok $es->create(
    index   => 'es_test_1',
    type    => 'type_1',
    id      => 1,
    data    => { text => 'foo', num => 123 },
    refresh => 1
)->{ok}, 'Create document';

throws_ok {
    $es->create(
        index => 'es_test_1',
        type  => 'type_1',
        id    => 1,
        data  => { text => 'foo', num => 123 }
    );
}
qr/ ElasticSearch::Error::Conflict/, ' - create conflict';

ok $es->index(
    index   => 'es_test_1',
    type    => 'type_1',
    id      => 1,
    data    => { text => 'foo', num => 123 },
    refresh => 1
)->{ok}, 'Index document';

throws_ok {
    $es->index(
        index   => 'es_test_1',
        type    => 'type_1',
        id      => 1,
        version => 1,
        data    => { text => 'foo', num => 123 }
    );
}
qr/ ElasticSearch::Error::Conflict/, ' - index conflict 1';

throws_ok {
    $es->index(
        index   => 'es_test_1',
        type    => 'type_1',
        id      => 1,
        version => 3,
        data    => { text => 'foo', num => 123 }
    );
}
qr/ ElasticSearch::Error::Conflict/, ' - index conflict 2';

ok $es->index(
    index   => 'es_test_1',
    type    => 'type_1',
    id      => 1,
    version => 2,
    data    => { text => 'foo', num => 123 },
    refresh => 1
)->{ok}, ' - index correct version';

1
