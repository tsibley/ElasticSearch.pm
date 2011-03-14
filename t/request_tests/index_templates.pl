#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

### INDEX TEMPLATES ###
ok $es->create_index_template(
    name     => 'mytemplate',
    template => 'test*',
    settings => { number_of_shards => 1 }
    ),
    'index template - create';

$es->create_index( index => 'test1' );
$es->create_index( index => 'std1' );

$r = $es->cluster_state->{metadata}{indices};

is $r->{test1}{settings}{'index.number_of_shards'}, 1,
    ' - index 1 has 1 shard';
is $r->{std1}{settings}{'index.number_of_shards'}, 5,
    ' - index 2 has 5 shards';

$es->delete_index( index => 'test1' );
$es->delete_index( index => 'std1' );

is $es->index_template( name => 'mytemplate' )
    ->{mytemplate}{settings}{'index.number_of_shards'}, 1,
    ' - template retrieved';

ok $es->delete_index_template( name => 'mytemplate' )->{ok},
    'Delete template';

ok !defined $es->index_template( name => 'mytemplate' )->{mytemplate},
    ' - template deleted';

throws_ok { $es->delete_index_template( name => 'mytemplate' ) }
qr/ElasticSearch::Error::Missing/, ' - template missing';
ok !$es->delete_index_template( name => 'mytemplate', ignore_missing => 1 ),
    ' - ignore missing';
1
