#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

### CREATE INDEX ###
ok $es->create_index( index => 'es_test_1' )->{ok}, 'Created index';
throws_ok { $es->create_index( index => 'es_test_1' ) } qr/Already exists/,
    ' - second create fails';

throws_ok { $es->create_index( index => [ 'es_test_1', 'es_test_2' ] ) }
qr/must be a single value/,
    ' - multiple indices fails';

ok $r = $es->create_index(
    index => 'es_test_2',

    settings => {

        number_of_shards   => 3,
        number_of_replicas => 1,

        analysis => {
            filter => {
                my_filter => {
                    type      => 'stop',
                    stopwords => [ 'foo', 'bar' ]
                },
            },
            tokenizer => {
                my_tokenizer => {
                    type             => 'standard',
                    max_token_length => 900,
                }
            },
            analyzer => {
                my_analyzer => {
                    tokenizer => 'my_tokenizer',
                    filter    => [ 'standard', 'my_filter' ]
                }
            }
        }
    },

    mappings => {
        type_1 => {
            _source    => { enabled => 0 },
            properties => {
                text => { type => 'string', analyzer => 'my_analyzer' },
                num  => { type => 'integer' }
            }
        }
        }

)->{ok}, ' - with settings and mappings';

wait_for_es();

$r = $es->cluster_state->{metadata}{indices}{es_test_2};

is $r->{settings}{'index.number_of_shards'}, 3, ' - number of shards stored';
is $r->{settings}{'index.analysis.filter.my_filter.stopwords.0'}, 'foo',
    ' - analyzer stored';
is $r->{mappings}{type_1}{_source}{enabled}, 0, ' - mappings stored';
is $r->{mappings}{type_1}{properties}{text}{analyzer}, 'my_analyzer',
    ' - analyzer mapped';

1
