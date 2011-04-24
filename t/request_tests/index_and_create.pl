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

is $es->index(
    index        => 'es_test_1',
    type         => 'type_1',
    id           => 5,
    version      => 10,
    version_type => 'external',
    data         => { text => 'foo', num => 123 },
    refresh      => 1
)->{_version}, 10, ' - index version_type external';

is $es->index(
    index        => 'es_test_1',
    type         => 'type_1',
    id           => 6,
    version      => 10,
    version_type => 'external',
    data         => { text => 'foo', num => 123 },
    refresh      => 1
)->{_version}, 10, ' - create version_type external';

1
