#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

# SCROLL
ok $r = $es->search(
    query  => { match_all => {} },
    sort   => ['num'],
    fields => ['_id'],
    scroll => '5m',
    size   => 2
    ),
    'Scroll search';
my $scroll_id = $r->{_scroll_id};
ok $scroll_id, ' - has scroll ID';

is $r->{hits}{hits}[0]{_id}, 1, ' - first hit is ID 1';
is $r->{hits}{hits}[1]{_id}, 2, ' - second hit is ID 2';

for my $tranche ( 1 .. 14 ) {
    ok $r = $es->scroll( scroll_id => $scroll_id, scroll => '5m' ),
        " - tranche $tranche";
    my $first  = 1 + 2 * $tranche;
    my $second = $first + 1;
    if ( $tranche == 14 ) {
        $first  = 30;
        $second = undef;
    }
    is $r->{hits}{hits}[0]{_id}, $first, " - first hit is ID $first";
    is $r->{hits}{hits}[1]{_id}, $second,
        " - first hit is ID " . ( $second || 'undef' );

}

1;
