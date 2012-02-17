#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

ok $es->validate_query( query => { match_all => {} } ), ' - query ok';
ok !$es->validate_query( query => { foo => {} } ), ' - query not ok';

ok $es->validate_query( queryb => { -all => 1 } ), ' - queryb ok';

ok !$es->validate_query( queryb => { -foo => 1 } ), ' - queryb not ok';

ok $es->validate_query( q => '*' ), ' - q ok';
ok !$es->validate_query( q => 'foo:' ), ' - q not ok';

throws_ok { $es->validate_query( query => 'foo', queryb => 'foo' ) }
qr/Cannot specify/, ' - query and queryb';

throws_ok { $es->validate_query( q => 'foo', queryb => 'foo' ) }
qr/Cannot specify/, ' - q and queryb';

throws_ok { $es->validate_query( q => 'foo', query => 'foo' ) }
qr/Cannot specify/, ' - q and query';
