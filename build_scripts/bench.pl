#!/usr/local/bin/perl

use JSON::XS;
use ElasticSearch;

open $fh, '<', 'data' or die "Couldn't open file 'data': $!";
my @data = @{ decode_json( join( '', <$fh> ) ) };

print "Rows: " . ( 0 + @data ) . "\n";

use Time::HiRes qw(time);
use Benchmark qw(timeit cmpthese :hireswallclock);

my %es = (
    http => ElasticSearch->new( servers => '127.0.0.1:9200' ),
    lite => ElasticSearch->new(
        servers   => '127.0.0.1:9200',
        transport => 'httplite'
    ),
    thrift => ElasticSearch->new(
        servers   => '127.0.0.1:9500',
        transport => 'thrift'
    )
);

my %subs = (
    index => sub {
        my $es = shift;
        for (@data) {
            $es->index(
                index => 'foo',
                type  => 'bar',
                data  => { text => $_ }
            );
        }
    },
    bulk_1000  => sub { bulk( @_, 1000 ) },
    bulk_5000  => sub { bulk( @_, 5000 ) },
    bulk_10000 => sub { bulk( @_, 10000 ) },
);

eval { $es{http}->delete_index( index => 'foo' ) };
print "Initializing\n";
run( $es{http}, $subs{index} );

print "\n\nStarting benchmark:\n";
my %times;
for my $transport ( keys %es ) {
    for my $sub ( keys %subs ) {
        my $key = "$transport-$sub";
        print "Running $key\n";
        $times{$key} = run( $es{$transport}, $subs{$sub} );
    }
}

cmpthese( \%times, 'all' );

#===================================
sub bulk {
#===================================
    my $es   = shift;
    my $size = shift;
    my $i    = 0;
    my $result;
    while ( $i < @data ) {
        my $max = $i + $size - 1;
        $max = $#data if $max >= @data;
        my @tranche = map {
            {
                index =>
                    { index => 'foo', type => 'bar', data => { text => $_ } }
            }
        } @data[ $i .. $max ];
        $i += $size;
        $result = $es->bulk( \@tranche );
    }
}

#===================================
sub run {
#===================================
    my $es = shift;

    my $sub = shift;
    print " - creating index\n";
    $es->create_index( 'index' => 'foo' );
    sleep 3;

    my $start = time();
    print " - indexing\n";
    my $time = timeit( 5, sub { $sub->($es) } );
    printf " - indexing time: %.2f\n", time() - $start;

    $start = time();
    $es->refresh_index;
    printf " - refresh time: %.2f\n", time() - $start;
    printf " - records indexed: %d\n", $es->count( match_all => {} )->{count};
    sleep 2;

    # make sure really refreshed
    $es->refresh_index;
    printf " - records indexed: %d\n", $es->count( match_all => {} )->{count};
    print " - deleting index\n";

    $es->delete_index( index => 'foo' );
    sleep 2;
    return $time;
}
