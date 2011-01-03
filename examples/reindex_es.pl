#!/usr/bin/perl

use lib '/opt/apache/sites/Projects/ElasticSearch/lib/';

use strict;
use warnings;
use ElasticSearch();
use Parallel::ForkManager();

use Data::Dumper;

=head1 NAME

reindex_es.pl

=head1 DESCRIPTION

Demo ElasticSearch script to copy one index to another in parallerl,
either on the same cluster or a different cluster.

You can edit the variables at the top of the script to change the source
and destination indices, and the source and destination clusters.

=cut

our $Source_ES    = '127.0.0.1:9200';
our $Dest_ES      = '127.0.0.1:9200';
our $Source_Index = 'my_source';
our $Dest_Index   = 'my_dest';
our $Max_Kids     = 10;
our $Rows         = 1000;
our $Per_Kid      = 10 * $Rows;
our $Same_Cluster = 0;

my $source = ElasticSearch->new( servers => $Source_ES );
my $dest   = ElasticSearch->new( servers => $Dest_ES );
my $pm     = Parallel::ForkManager->new($Max_Kids);

$|++;    # Auto-flush STDOUT to see progress

main();

#===================================
sub main {
#===================================
    check_clusters();
    delete_index();
    create_index();

    my $total = total_docs();
    my $start = 0;
    while ( $start < $total ) {
        my $end = $start + $Per_Kid;
        if ( $pm->start ) {

            # parent
            $start = $end;
            next;
        }

        # child
        index_docs( $start, $end );
        $pm->finish;

    }
    $pm->wait_all_children;
    print "\n - Done - \n";
}

=head2 C<check_clusters()>

=cut

#===================================
sub check_clusters {
#===================================
    $Same_Cluster = $source->cluster_health->{cluster_name} eq
        $dest->cluster_health->{cluster_name};
}

#===================================
sub delete_index {
#===================================
    die "The source and dest indices are the same - cannot proceed"
        if $Same_Cluster && $Source_Index eq $Dest_Index;

    print "Deleting index '$Dest_Index' in case it already exists\n";
    eval {
        $dest->delete_index( index => $Dest_Index );
        wait_for_es();
    };
}

#===================================
sub create_index {
#===================================
    print "Creating index '$Dest_Index'\n";
    my $defn = $source->cluster_state->{metadata}{indices}{$Source_Index}
        or die "Couldn't find index '$Source_Index'. "
        . "Does it exist? Is it an alias?";

    # we don't want to use the same alias on the same cluster
    delete $defn->{aliases}
        if $Same_Cluster;

    $dest->create_index( index => $Dest_Index, defn => $defn );
    wait_for_es();

}

#===================================
sub total_docs {
#===================================
    my $total = $source->count(
        index     => $Source_Index,
        match_all => {}
    )->{count};

    print "Indexing $total docs from '$Source_Index' to '$Dest_Index\n";
    return $total;
}

#===================================
sub index_docs {
#===================================
    my $start = shift;
    my $end   = shift;

    while ( $start < $end ) {
        print ".";

        my $objects = $source->search(
            index => $Source_Index,
            query => { match_all => {} },
            sort  => ['_id'],
            from  => $start,
            size  => $Rows
        )->{hits}{hits};

        # Already have _type _id _routing _parent
        # but change _index to point to the dest index
        $_->{_index} = $Dest_Index for @$objects;

        my $result = $dest->bulk_create($objects);
        die Dumper( $result->{errors} ) if $result->{errors};

        last if @$objects < $Rows;
        $start += $Rows;
    }
}

#===================================
sub wait_for_es {
#===================================
    $dest->cluster_health( wait_for_status => 'yellow' );
}
