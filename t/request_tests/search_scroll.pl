#!perl

use Test::More;
use strict; use warnings;
our $es;
my $r;

    # SCROLL
    ok $r = $es->search(
        query  => { match_all => {} },
        sort   => ['num'],
        scroll => '5m',
        size   => 2
        ),
        'Scroll search';
    my $scroll_id = $r->{_scroll_id};
    ok $scroll_id, ' - has scroll ID';

    is $r->{hits}{hits}[0]{_id}, 1, ' - first hit is ID 1';
    is $r->{hits}{hits}[1]{_id}, 2, ' - second hit is ID 2';

TODO: {
        local $TODO = "Scroll ID tests - broken on server";
        ok $r = $es->scroll( scroll_id => $scroll_id ), ' - next tranche';
        is $r->{hits}{hits}[0]{_id}, 3, ' - first hit is ID 3';
        is $r->{hits}{hits}[1]{_id}, 4, ' - second hit is ID 4';

        ok $r = $es->scroll( scroll_id => $scroll_id ), ' - next tranche';
        is $r->{hits}{hits}[0]{_id}, 3, ' - first hit is ID 5';
        is $r->{hits}{hits}[1]{_id}, 4, ' - second hit is ID 6';
    }

1;