#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

### INDEX ALIASES ###
ok $es->aliases(
    actions => { add => { alias => 'alias_1', index => 'es_test_1' } }
    ),
    'add alias_1';
wait_for_es();

is $es->get_aliases->{aliases}{alias_1}[0], 'es_test_1', 'alias_1 added';
ok $es->aliases(
    actions => [
        { add    => { alias => 'alias_1', index => 'es_test_2' } },
        { remove => { alias => 'alias_1', index => 'es_test_1' } }
    ]
    ),
    'add and remove alias_1';

wait_for_es();

is $es->get_aliases->{aliases}{alias_1}[0], 'es_test_2', 'alias_1 changed';

1
