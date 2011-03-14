#!perl

use Test::More;
use strict;
use warnings;
our ( $es, $instances );
my $r;

### NODES ###
isa_ok $r = $es->nodes, 'HASH', 'All nodes';

is $r->{cluster_name}, 'es_test', ' - has cluster_name';
isa_ok $r->{nodes},    'HASH',    ' - has nodes';

my @nodes = ( keys %{ $r->{nodes} } );
is @nodes, $instances, " - $instances nodes";

my $first = shift @nodes;
ok $r= $es->nodes( node => $first ), ' - request single node';
is keys %{ $r->{nodes} }, 1, ' - got one node';
ok $r->{nodes}{$first}, ' - got same node';

isa_ok $r = $es->nodes( node => \@nodes ), 'HASH', ' - nodes by name';
is keys %{ $r->{nodes} }, @nodes, ' - retrieved same number of nodes';
is_deeply [ keys %{ $r->{nodes} } ], \@nodes, ' - retrieved the same nodes';

ok !$es->nodes()->{nodes}{$first}{settings}, ' - without settings';

isa_ok $es->nodes( settings => 1 )->{nodes}{$first}{settings}, 'HASH',
    ' - with settings';

isa_ok $r= $es->nodes_stats->{nodes}, 'HASH', ' - nodes_stats';

ok $r->{$first}{jvm}, ' - stats detail';
1
