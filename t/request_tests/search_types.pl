#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

# QUERY_THEN_FETCH
isa_ok $r= $es->search(
    query       => { match_all => {} },
    search_type => 'query_then_fetch'
    ),
    'HASH',
    "query_then_fetch";
is $r->{hits}{total}, 29, ' - total correct';
is @{ $r->{hits}{hits} }, 10, ' - returned 10 results';

# QUERY_AND_FETCH

isa_ok $r= $es->search(
    query       => { match_all => {} },
    search_type => 'query_and_fetch'
    ),
    'HASH',
    "query_and_fetch";
is $r->{hits}{total}, 29, ' - total correct';
ok @{ $r->{hits}{hits} } > 10, ' - returned  > 10 results';

1;
