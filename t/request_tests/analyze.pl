#!perl

use Test::More;
use strict;
use warnings;
our $es;
my $r;

### ANALYZER ###
is $es->analyze(
    index  => 'es_test_1',
    text   => 'tHE BLACK and white! AND red',
    format => 'text',
    prefer_local => 0,
    )->{tokens},
    "[black:4->9:<ALPHANUM>]\n\n4: \n[white:14->19:<ALPHANUM>]\n\n6: \n[red:25->28:<ALPHANUM>]\n",
    'Analyzer';
1;
